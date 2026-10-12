// Package revoke 处理数据源撤销（governance 详设 6.2）：governance 撤销时先写按数据源的紧急拒绝（在线立即不可见），
// 再发出 source.revoked 平台事件；connectors 的管理服务消费它，对这个源在索引里的全部在用文档写删除标记
// （之后按观察期物理删除并留删除凭证），并把源标为已撤销，之后的每轮同步都整轮跳过，不会再入库。
//
// 删除标记直接经 indexing 的 TombstoneDocument 写入，不走事件流与 processing：撤销属收紧类，要尽快生效，
// 而 processing 是批处理，不一定在运行。TombstoneDocument 幂等，重复处理同一事件不改变结果。
package revoke

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/outbox"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/consume"
)

// Group 是 connectors 在平台事件流上的消费组。
const Group = "connectors-revocations"

// MaxAttempts 是同一事件在本进程里的重试上限（每次间隔 10 秒）。超过后不再重试，事件留在未确认列表里，
// 管理服务重启时再做一轮；在线已有紧急拒绝挡住，不会因此泄露。
const MaxAttempts = 30

// Index 是 indexing 的 IndexWriterService 中撤销用到的部分。
type Index interface {
	ListSourceDocuments(ctx context.Context, in *ingestionv1.ListSourceDocumentsRequest,
		opts ...grpc.CallOption) (grpc.ServerStreamingClient[ingestionv1.ListSourceDocumentsResponse], error)
	TombstoneDocument(ctx context.Context, in *ingestionv1.TombstoneDocumentRequest,
		opts ...grpc.CallOption) (*ingestionv1.TombstoneDocumentResponse, error)
}

// Marker 记下数据源已撤销（syncstatus.Store.SetRevoked）。
type Marker interface {
	SetRevoked(ctx context.Context, sourceID string, at time.Time) error
}

// Auditor 写变更审计（libs/platform-go/changeaudit）。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

// Revoker 对一个数据源执行撤销。
type Revoker struct {
	Index  Index
	Status Marker
	Redis  redis.Cmdable
	// Audit 记下撤销的执行结果（source.revoke_applied）：撤销本身由 governance 审计，这里补上“删了哪些、何时删完”
	Audit Auditor
	Log   *slog.Logger
}

// Revoke 先标为已撤销（之后的同步不再运行），再给全部在用文档写删除标记，最后清掉已发出状态。
// 中途失败返回错误，事件不确认，之后重做；已写的删除标记重复写是幂等的。
func (r *Revoker) Revoke(ctx context.Context, sourceID string, at time.Time) (int, error) {
	if err := r.Status.SetRevoked(ctx, sourceID, at); err != nil {
		return 0, fmt.Errorf("标记已撤销: %w", err)
	}
	stream, err := r.Index.ListSourceDocuments(ctx, &ingestionv1.ListSourceDocumentsRequest{SourceId: sourceID})
	if err != nil {
		return 0, err
	}
	var active []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("读取已入库文档: %w", err)
		}
		for _, d := range resp.GetDocuments() {
			if !d.GetTombstoned() {
				active = append(active, d.GetDocId())
			}
		}
	}
	for _, id := range active {
		if _, err := r.Index.TombstoneDocument(ctx, &ingestionv1.TombstoneDocumentRequest{
			SourceId: sourceID, DocId: id, OccurredAt: timestamppb.New(at),
		}); err != nil {
			return 0, fmt.Errorf("文档 %s 写删除标记: %w", id, err)
		}
	}
	// 审计写不进去就不算完成：事件不确认，之后重做（删除标记幂等）
	if err := r.Audit.Write(ctx, "source.revoke_applied", sourceID, map[string]any{"tombstoned": len(active),
		"revoked_at": at}); err != nil {
		return 0, fmt.Errorf("写变更审计: %w", err)
	}
	// 已发出状态与表结构源的同步记录都清掉：源已撤销，不会再按它们比较
	prefix := "bf:connector:" + sourceID
	if err := r.Redis.Del(ctx, prefix+":state", prefix+":synced").Err(); err != nil {
		return 0, fmt.Errorf("清理同步状态: %w", err)
	}
	return len(active), nil
}

// Consumer 处理平台事件流里的 source.revoked，其余事件直接确认（消费循环见 internal/consume）。
type Consumer struct {
	Revoker *Revoker
	Log     *slog.Logger
}

// Loop 返回平台事件流上的消费循环。
func (c *Consumer) Loop(rdb *redis.Client, name string) *consume.Loop {
	return &consume.Loop{Redis: rdb, Stream: outbox.Stream, Group: Group, Name: name, MaxAttempts: MaxAttempts,
		Retry: 10 * time.Second, Handle: c.Process, What: "撤销数据源", Log: c.Log}
}

// Process 处理一条消息，返回是否可以确认。
func (c *Consumer) Process(ctx context.Context, values map[string]any) bool {
	evt, err := outbox.Decode(values)
	if err != nil {
		c.Log.ErrorContext(ctx, "丢弃无法解析的平台事件", slog.Any("err", err))
		return true
	}
	if evt.GetType() != outbox.SourceRevoked {
		return true
	}
	src := evt.GetObjectId()
	if !strings.HasPrefix(src, "src_") {
		c.Log.ErrorContext(ctx, "source.revoked 事件的数据源 ID 不合法，丢弃", slog.String("event_id", evt.GetEventId()))
		return true
	}
	n, err := c.Revoker.Revoke(ctx, src, evt.GetOccurredAt().AsTime())
	if err != nil {
		c.Log.ErrorContext(ctx, "撤销数据源失败，稍后重试", slog.String("source_id", src), slog.Any("err", err))
		return false
	}
	c.Log.WarnContext(ctx, "数据源已撤销，全部文档已写删除标记", slog.String("source_id", src), slog.Int("documents", n),
		slog.String("event_id", evt.GetEventId()))
	return true
}
