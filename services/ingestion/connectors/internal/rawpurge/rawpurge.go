// Package rawpurge 在文档的索引侧副本物理删除之后，删掉 connectors 持有的对象存储原文与文档包（技术设计文档 9.2，
// 评估报告 K6）：消费 indexing 发出的 DocumentPurged（事件流 ingest:document-purged），删掉
// raw/<数据源>/<文档>/ 下的全部对象，再经 IndexWriterService.RecordPurge 把完成时间补记进同一份删除凭证。
// 删除幂等：对象已不存在时同样补记。
package rawpurge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/consume"
)

const (
	// Stream 与 indexing 的 writer.PurgeStream 相同
	Stream = "ingest:document-purged"
	Group  = "connectors-purge"
	// MaxAttempts 是同一事件在本进程里的重试上限（间隔 30 秒）
	MaxAttempts = 20
)

// Objects 是对象存储里按前缀删除。
type Objects interface {
	DeletePrefix(ctx context.Context, prefix string) (int, error)
}

// Index 是 indexing 的 IndexWriterService 中这里用到的部分。
type Index interface {
	RecordPurge(ctx context.Context, in *ingestionv1.RecordPurgeRequest, opts ...grpc.CallOption) (*ingestionv1.RecordPurgeResponse, error)
}

type Purger struct {
	Objects Objects
	Index   Index
	Log     *slog.Logger
	Now     func() time.Time
}

// Loop 返回事件流上的消费循环。
func (p *Purger) Loop(rdb *redis.Client, name string) *consume.Loop {
	return &consume.Loop{Redis: rdb, Stream: Stream, Group: Group, Name: name, MaxAttempts: MaxAttempts,
		Retry: 30 * time.Second, Handle: p.Process, What: "删除对象存储里的原文", Log: p.Log}
}

// Process 处理一条 DocumentPurged，返回是否可以确认。格式不对的消息记日志后确认（重读也解析不了）。
func (p *Purger) Process(ctx context.Context, values map[string]any) bool {
	raw, ok := values["pb"].(string)
	evt := &ingestionv1.DocumentPurged{}
	if !ok || proto.Unmarshal([]byte(raw), evt) != nil || !valid(evt) {
		p.Log.ErrorContext(ctx, "丢弃无法解析的 DocumentPurged")
		return true
	}
	n, err := p.Purge(ctx, evt)
	if err != nil {
		p.Log.WarnContext(ctx, "删除对象存储里的原文失败，稍后重试", slog.String("doc_id", evt.GetDocId()), slog.Any("err", err))
		return false
	}
	p.Log.InfoContext(ctx, "已删除对象存储里的原文", slog.String("doc_id", evt.GetDocId()), slog.String("receipt_id", evt.GetReceiptId()),
		slog.Int("objects", n))
	return true
}

func valid(e *ingestionv1.DocumentPurged) bool {
	return strings.HasPrefix(e.GetSourceId(), "src_") && e.GetDocId() != "" && !strings.ContainsAny(e.GetDocId(), "/\\") &&
		strings.HasPrefix(e.GetReceiptId(), "del_")
}

// Purge 删掉这份文档在对象存储里的全部对象（各内容版本的原文与文档包），再补记删除凭证。
func (p *Purger) Purge(ctx context.Context, e *ingestionv1.DocumentPurged) (int, error) {
	n, err := p.Objects.DeletePrefix(ctx, fmt.Sprintf("raw/%s/%s/", e.GetSourceId(), e.GetDocId()))
	if err != nil {
		return 0, fmt.Errorf("删除对象: %w", err)
	}
	_, err = p.Index.RecordPurge(ctx, &ingestionv1.RecordPurgeRequest{ReceiptId: e.GetReceiptId(), Store: "object_store",
		CompletedAt: timestamppb.New(p.Now())})
	if err != nil {
		return n, errors.Join(errors.New("补记删除凭证"), err)
	}
	return n, nil
}

// S3 是 Objects 的默认实现（S3 兼容的对象存储）。
type S3 struct {
	Client *minio.Client
	Bucket string
}

func (s *S3) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	listed := s.Client.ListObjects(ctx, s.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	// 列出的对象转给批量删除；计数与列举错误只在这个协程里写，done 关闭后再读
	toRemove := make(chan minio.ObjectInfo)
	done := make(chan struct{})
	n := 0
	var listErr error
	go func() {
		defer close(done)
		defer close(toRemove)
		for o := range listed {
			if o.Err != nil {
				if listErr == nil {
					listErr = fmt.Errorf("列出对象: %w", o.Err)
				}
				continue
			}
			n++
			toRemove <- o
		}
	}()
	var removeErr error
	for e := range s.Client.RemoveObjects(ctx, s.Bucket, toRemove, minio.RemoveObjectsOptions{}) {
		if e.Err != nil && removeErr == nil {
			removeErr = fmt.Errorf("删除 %s: %w", e.ObjectName, e.Err)
		}
	}
	<-done
	return n, errors.Join(listErr, removeErr)
}
