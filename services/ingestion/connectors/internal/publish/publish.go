// Package publish 把文档写成对象存储里的文档包，并在事件流上发出 DocumentChanged（入库管道详设 14.2）。
// 事件只带 ID、哈希与引用，不带正文；至少投递一次，下游按 event_id 与内容摘要幂等处理。
package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
)

// Stream 是内容变更（upsert）的事件流，processing 消费；AccessStream 是仅权限变化与删除的事件流，
// indexing 直接消费（入库管道详设第 9 节：不与内容入库排队）。消息键为 doc_id。
const (
	Stream       = "ingest:document-changed"
	AccessStream = "ingest:document-access"
)

var classifications = map[string]commonv1.Classification{
	"public": commonv1.Classification_CLASSIFICATION_PUBLIC, "internal": commonv1.Classification_CLASSIFICATION_INTERNAL,
	"confidential": commonv1.Classification_CLASSIFICATION_CONFIDENTIAL, "restricted": commonv1.Classification_CLASSIFICATION_RESTRICTED,
}

// Envelope 是文档包：原文加上源系统给出的元数据与归一化后的 ACL。processing 按 raw_ref 取用。
// 文本类来源（Wiki）的正文直接放在 content 里；二进制原文（PDF、Office、邮件、图片）另存一个对象，
// content_ref 指向它，文档包本身保持很小。
type Envelope struct {
	SourceID       string    `json:"source_id"`
	DocID          string    `json:"doc_id"`
	Title          string    `json:"title"`
	SourceURI      string    `json:"source_uri"`
	SourceType     string    `json:"source_type"`
	Owner          string    `json:"owner"`
	Classification string    `json:"classification"`
	ACLTokens      []string  `json:"acl_tokens"`
	UpdatedAt      time.Time `json:"updated_at"`
	ContentType    string    `json:"content_type"`
	Content        string    `json:"content,omitempty"`
	ContentRef     string    `json:"content_ref,omitempty"`
	FileName       string    `json:"file_name,omitempty"`
	ContentHash    string    `json:"content_hash"`
	// 源系统的结构化字段，processing 据此抽取文档元数据（入库管道详设第 7 节）
	SourceFields *docmeta.SourceFields `json:"source_fields,omitempty"`
}

// Document 是要发布的一份文档。Text 与 Raw 二选一：前者是文本正文，后者是二进制原文。
type Document struct {
	docmeta.Meta
	SourceURI   string
	SourceType  string
	ContentType string
	FileName    string
	Text        string
	Raw         []byte
	ContentHash string
}

type Publisher struct {
	S3       *minio.Client
	Bucket   string
	Redis    *redis.Client
	SourceID string
	// Detection 是事件里的发现方式；为零时是清单比对（listing）。推送入库为 push
	Detection ingestionv1.Detection
}

// Publish 存文档包并发事件。对象键含内容摘要：内容不变时重复写的是同一个对象。
// change 为 UPSERT 或 ACL_ONLY；ACL_ONLY 的产生时间取发现时间（清单只能告诉我们权限现在变了）。
func (p *Publisher) Publish(ctx context.Context, d *Document, change ingestionv1.ChangeType, detectedAt time.Time) error {
	return p.PublishWithID(ctx, NewEventID(), d, change, detectedAt)
}

// PublishWithID 与 Publish 相同，事件 ID 由调用方给出（推送入库要把它返回给推送方）。
func (p *Publisher) PublishWithID(ctx context.Context, eventID string, d *Document, change ingestionv1.ChangeType, detectedAt time.Time) error {
	if (d.Text == "") == (len(d.Raw) == 0) {
		return errors.New("文档的 Text 与 Raw 必须恰好给出一个")
	}
	prefix := fmt.Sprintf("raw/%s/%s/%s", p.SourceID, d.DocID, d.ContentHash)
	env := Envelope{
		SourceID: p.SourceID, DocID: d.DocID, Title: d.Title, SourceURI: d.SourceURI,
		SourceType: d.SourceType, Owner: d.Owner, Classification: d.Classification, ACLTokens: d.ACLTokens,
		UpdatedAt: d.UpdatedAt, ContentType: d.ContentType, Content: d.Text, FileName: d.FileName,
		ContentHash: d.ContentHash, SourceFields: d.Source,
	}
	if len(d.Raw) > 0 {
		key := prefix + path.Ext(d.FileName)
		if err := p.put(ctx, key, d.Raw, d.ContentType); err != nil {
			return fmt.Errorf("写入原文: %w", err)
		}
		env.ContentRef = "s3://" + p.Bucket + "/" + key
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	key := prefix + ".json"
	if err := p.put(ctx, key, body, "application/json"); err != nil {
		return fmt.Errorf("写入文档包: %w", err)
	}
	evt, err := p.changed(eventID, d, change, detectedAt, "s3://"+p.Bucket+"/"+key)
	if err != nil {
		return err
	}
	return p.emit(ctx, evt)
}

// changed 生成内容变更或仅权限变化的事件。快速通道不读文档包：仅权限变化时新权限与密级随事件带过去，不带 raw_ref；
// 权限为空即无人可见。
func (p *Publisher) changed(eventID string, d *Document, change ingestionv1.ChangeType, detectedAt time.Time, rawRef string) (*ingestionv1.DocumentChanged, error) {
	evt := &ingestionv1.DocumentChanged{
		EventId: eventID, SourceId: p.SourceID, DocId: d.DocID,
		ChangeType: change, ContentHash: d.ContentHash,
		AclFingerprint: d.ACLFingerprint, OccurredAt: timestamppb.New(occurredAt(d, change, detectedAt)),
		DetectedAt: timestamppb.New(detectedAt), Detection: p.detection(),
		RawRef: rawRef,
	}
	if change == ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY {
		c, ok := classifications[d.Classification]
		if !ok {
			return nil, fmt.Errorf("密级 %q 不合法", d.Classification)
		}
		evt.RawRef, evt.AclTokens, evt.Classification = "", d.ACLTokens, c
	}
	return evt, nil
}

func occurredAt(d *Document, change ingestionv1.ChangeType, detectedAt time.Time) time.Time {
	if change == ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY {
		return detectedAt
	}
	return d.UpdatedAt
}

// Delete 发删除事件：源端已不存在该文档。删除时间取发现时间（清单只能告诉我们“现在没有了”）。
func (p *Publisher) Delete(ctx context.Context, docID string, detectedAt time.Time) error {
	return p.DeleteWithID(ctx, NewEventID(), docID, detectedAt)
}

// DeleteWithID 与 Delete 相同，事件 ID 由调用方给出。
func (p *Publisher) DeleteWithID(ctx context.Context, eventID, docID string, detectedAt time.Time) error {
	return p.emit(ctx, &ingestionv1.DocumentChanged{
		EventId: eventID, SourceId: p.SourceID, DocId: docID,
		ChangeType: ingestionv1.ChangeType_CHANGE_TYPE_DELETE,
		OccurredAt: timestamppb.New(detectedAt), DetectedAt: timestamppb.New(detectedAt),
		Detection: p.detection(),
	})
}

func (p *Publisher) detection() ingestionv1.Detection {
	if p.Detection == ingestionv1.Detection_DETECTION_UNSPECIFIED {
		return ingestionv1.Detection_DETECTION_LISTING
	}
	return p.Detection
}

// PartitionGrants 发出分区授权变化（总体设计 ADR-14）：与仅权限变化同一条快速通道（kind = partition_grant），
// indexing 据此整个替换这个分区的被授权方。
func (p *Publisher) PartitionGrants(ctx context.Context, partition string, grantees []string, at time.Time) error {
	pb, err := proto.Marshal(&ingestionv1.PartitionGrantChanged{EventId: NewEventID(), SourceId: p.SourceID, Partition: partition,
		Grantees: grantees, OccurredAt: timestamppb.New(at)})
	if err != nil {
		return err
	}
	return p.Redis.XAdd(ctx, &redis.XAddArgs{Stream: AccessStream, Values: map[string]any{"kind": "partition_grant", "pb": pb}}).Err()
}

func (p *Publisher) emit(ctx context.Context, evt *ingestionv1.DocumentChanged) error {
	pb, err := proto.Marshal(evt)
	if err != nil {
		return err
	}
	stream := Stream
	if evt.GetChangeType() != ingestionv1.ChangeType_CHANGE_TYPE_UPSERT {
		stream = AccessStream
	}
	return p.Redis.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"doc_id": evt.GetDocId(), "pb": pb}}).Err()
}

func (p *Publisher) put(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := p.S3.PutObject(ctx, p.Bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	return err
}

// NewEventID 生成事件 ID（evt_ 前缀）。
func NewEventID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "evt_" + hex.EncodeToString(b)
}
