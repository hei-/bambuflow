// Package access 是权限与删除的快速通道（入库管道详设第 9 节）：直接消费 connectors 发到 ingest:document-access 的
// 删除、仅权限变化与分区授权变化事件，写删除标记、更新文档级权限或替换分区授权，对线上立即生效。不经过 processing、不与内容入库排队，
// processing 不可用或积压时撤权照常生效。
//
// 失败一直重试（撤权与删除必须生效，不进死信），间隔从 1 秒翻倍到最长 30 秒；处理完才确认。启动时与之后每分钟
// 认领空闲超过 5 分钟的挂起消息（换了主机名的旧 Pod 没做完的，streams.ClaimIdle）。
package access

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/streams"
)

const (
	Stream = "ingest:document-access"
	Group  = "indexing-access"
)

// Writer 是写删除标记与更新权限（writer.Server）。
type Writer interface {
	TombstoneDocument(ctx context.Context, r *ingestionv1.TombstoneDocumentRequest) (*ingestionv1.TombstoneDocumentResponse, error)
	UpdateAcl(ctx context.Context, r *ingestionv1.UpdateAclRequest) (*ingestionv1.UpdateAclResponse, error)
	SetPartitionGrants(ctx context.Context, r *ingestionv1.SetPartitionGrantsRequest) (*ingestionv1.SetPartitionGrantsResponse, error)
}

// KindPartitionGrant 是分区授权变化的消息（字段 kind），pb 为 PartitionGrantChanged；没有 kind 的是 DocumentChanged
const KindPartitionGrant = "partition_grant"

type Consumer struct {
	Redis  *redis.Client
	Writer Writer
	// 消费者名，取主机名
	Name string
	Log  *slog.Logger
}

const maxBackoff = 30 * time.Second

// Run 一直运行到 ctx 取消。Redis 暂时不可用时等它恢复，不退出：快速通道停了撤权就不生效。
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		err := c.Redis.XGroupCreateMkStream(ctx, Stream, Group, "0").Err()
		if err == nil || strings.HasPrefix(err.Error(), "BUSYGROUP") {
			break
		}
		c.Log.WarnContext(ctx, "创建权限与删除事件流的消费组失败，稍后重试", slog.Any("err", err))
		sleep(ctx, 5*time.Second)
	}
	// "0" 读本消费者未确认的消息，读空后切到 ">" 读新消息；失败时切回 "0" 退避后重读
	start, backoff := "0", time.Second
	var nextClaim time.Time
	for ctx.Err() == nil {
		if now := time.Now(); now.After(nextClaim) {
			nextClaim = now.Add(time.Minute)
			if n, err := streams.ClaimIdle(ctx, c.Redis, Stream, Group, c.Name, 5*time.Minute); err != nil {
				c.Log.WarnContext(ctx, "认领挂起消息失败", slog.Any("err", err))
			} else if n > 0 {
				c.Log.InfoContext(ctx, "认领了别的消费者没做完的权限与删除事件", slog.Int("count", n))
				start = "0"
			}
		}
		res, err := c.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: Group, Consumer: c.Name, Streams: []string{Stream, start}, Count: 50, Block: 5 * time.Second,
		}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				c.Log.WarnContext(ctx, "读取权限与删除事件流失败", slog.Any("err", err))
				sleep(ctx, time.Second)
			}
			continue
		}
		msgs := res[0].Messages
		if start == "0" && len(msgs) == 0 {
			start = ">"
			continue
		}
		for _, m := range msgs {
			if !c.Process(ctx, m.Values) {
				start = "0"
				sleep(ctx, backoff)
				backoff = min(backoff*2, maxBackoff)
				break
			}
			backoff = time.Second
			if err := c.Redis.XAck(ctx, Stream, Group, m.ID).Err(); err != nil {
				c.Log.WarnContext(ctx, "确认事件失败，稍后重读", slog.Any("err", err))
			}
		}
	}
}

// Process 处理一条事件，返回是否可以确认。格式不对、重试也不会成功的事件记错误日志后确认；
// 写入暂时失败的不确认，之后重读。
func (c *Consumer) Process(ctx context.Context, values map[string]any) bool {
	raw, _ := values["pb"].(string)
	if kind, _ := values["kind"].(string); kind == KindPartitionGrant {
		return c.partition(ctx, raw)
	}
	var evt ingestionv1.DocumentChanged
	if err := proto.Unmarshal([]byte(raw), &evt); err != nil {
		c.Log.ErrorContext(ctx, "丢弃无法解析的权限与删除事件", slog.Any("err", err))
		return true
	}
	var err error
	switch evt.GetChangeType() {
	case ingestionv1.ChangeType_CHANGE_TYPE_DELETE:
		_, err = c.Writer.TombstoneDocument(ctx, &ingestionv1.TombstoneDocumentRequest{
			SourceId: evt.GetSourceId(), DocId: evt.GetDocId(), OccurredAt: evt.GetOccurredAt()})
	case ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY:
		tokens, class := evt.GetAclTokens(), evt.GetClassification()
		if class == commonv1.Classification_CLASSIFICATION_UNSPECIFIED {
			// 缺密级的事件按最严处理：无人可见、受限级，不能因为字段缺失而放宽（入库管道详设第 9 节）
			c.Log.ErrorContext(ctx, "仅权限变化的事件缺少密级，按无人可见处理", slog.String("doc_id", evt.GetDocId()))
			tokens, class = nil, commonv1.Classification_CLASSIFICATION_RESTRICTED
		}
		_, err = c.Writer.UpdateAcl(ctx, &ingestionv1.UpdateAclRequest{SourceId: evt.GetSourceId(), DocId: evt.GetDocId(),
			AclTokens: tokens, Classification: class, OccurredAt: evt.GetOccurredAt()})
	default:
		c.Log.ErrorContext(ctx, "权限与删除事件流上出现了别的变更类型，丢弃", slog.String("doc_id", evt.GetDocId()),
			slog.String("change_type", evt.GetChangeType().String()))
		return true
	}
	switch {
	case err == nil:
		return true
	case status.Code(err) == codes.InvalidArgument:
		c.Log.ErrorContext(ctx, "权限与删除事件不合法，丢弃", slog.String("doc_id", evt.GetDocId()), slog.Any("err", err))
		return true
	default:
		c.Log.WarnContext(ctx, "处理权限与删除事件失败，稍后重试", slog.String("doc_id", evt.GetDocId()), slog.Any("err", err))
		return false
	}
}

// partition 处理分区授权变化（总体设计 ADR-14）：整个分区替换，失败重试与权限事件相同。
func (c *Consumer) partition(ctx context.Context, raw string) bool {
	var evt ingestionv1.PartitionGrantChanged
	if err := proto.Unmarshal([]byte(raw), &evt); err != nil {
		c.Log.ErrorContext(ctx, "丢弃无法解析的分区授权事件", slog.Any("err", err))
		return true
	}
	_, err := c.Writer.SetPartitionGrants(ctx, &ingestionv1.SetPartitionGrantsRequest{SourceId: evt.GetSourceId(),
		Partition: evt.GetPartition(), Grantees: evt.GetGrantees(), OccurredAt: evt.GetOccurredAt()})
	switch {
	case err == nil:
		return true
	case status.Code(err) == codes.InvalidArgument:
		c.Log.ErrorContext(ctx, "分区授权事件不合法，丢弃", slog.String("source_id", evt.GetSourceId()), slog.Any("err", err))
		return true
	default:
		c.Log.WarnContext(ctx, "处理分区授权事件失败，稍后重试", slog.String("source_id", evt.GetSourceId()), slog.Any("err", err))
		return false
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
