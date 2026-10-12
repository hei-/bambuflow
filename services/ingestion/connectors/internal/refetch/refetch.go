// Package refetch 消费 RefetchRequested（入库管道详设第 5、14 节）：indexing 撤销误写的删除标记后，
// 要求本数据源重新从源系统拉取这些文档。做法是在本轮同步前把它们从已同步状态里去掉，
// 同步时源端仍存在的就被当作新文档重新发出 DocumentChanged；源端已不存在的不会恢复。
package refetch

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/streams"
)

// StreamKey 是 indexing 发出 RefetchRequested 的事件流。
const StreamKey = "ingest:refetch-requested"

// Stream 读取事件流：pending 为 true 时读本消费者已读未确认的消息。
type Stream interface {
	Read(ctx context.Context, pending bool) ([]redis.XMessage, error)
	Ack(ctx context.Context, id string) error
}

// State 是已同步状态（doc_id → 指纹）。
type State interface {
	Forget(ctx context.Context, docIDs []string) error
}

// Apply 处理事件流里的全部 RefetchRequested，返回从状态里去掉的文档数。
// 先改状态再确认消息：中途失败时消息留在挂起列表里，下次重做（去掉状态是幂等的）。
// 别的数据源的事件直接确认（每个数据源一个消费组，各自都会收到）；格式不对的记错误日志后确认，不让一条坏消息卡住后面的。
func Apply(ctx context.Context, s Stream, st State, sourceID string, log *slog.Logger) (int, error) {
	n := 0
	for _, pending := range []bool{true, false} {
		for {
			msgs, err := s.Read(ctx, pending)
			if err != nil {
				return n, err
			}
			for _, m := range msgs {
				evt, err := decode(m)
				switch {
				case err != nil:
					log.ErrorContext(ctx, "RefetchRequested 格式不对，丢弃", slog.String("id", m.ID), slog.Any("err", err))
				case evt.GetSourceId() == sourceID && len(evt.GetDocIds()) > 0:
					if err := st.Forget(ctx, evt.GetDocIds()); err != nil {
						return n, err
					}
					n += len(evt.GetDocIds())
					log.InfoContext(ctx, "按 RefetchRequested 重新拉取", slog.String("event_id", evt.GetEventId()),
						slog.Int("docs", len(evt.GetDocIds())), slog.String("reason", evt.GetReason().String()))
				}
				if err := s.Ack(ctx, m.ID); err != nil {
					return n, err
				}
			}
			// 挂起列表只读一轮（确认之前会一直读到同样的消息）；新消息读空为止
			if pending || len(msgs) == 0 {
				break
			}
		}
	}
	return n, nil
}

func decode(m redis.XMessage) (*ingestionv1.RefetchRequested, error) {
	raw, ok := m.Values["pb"].(string)
	if !ok {
		return nil, errors.New("缺少 pb 字段")
	}
	evt := &ingestionv1.RefetchRequested{}
	if err := proto.Unmarshal([]byte(raw), evt); err != nil {
		return nil, err
	}
	return evt, nil
}

// RedisStream 是 Stream 的实现：每个数据源一个消费组（connectors-refetch-<source_id>）。
type RedisStream struct {
	Client   redis.Cmdable
	SourceID string
	Consumer string
	started  bool
}

func (r *RedisStream) group() string { return "connectors-refetch-" + r.SourceID }

func (r *RedisStream) Read(ctx context.Context, pending bool) ([]redis.XMessage, error) {
	if !r.started {
		err := r.Client.XGroupCreateMkStream(ctx, StreamKey, r.group(), "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return nil, err
		}
		// 每轮同步是新的 Pod、新的主机名：上一轮读了没确认的请求先认领过来（空闲 30 分钟以上，大于一轮同步的时长）
		if _, err := streams.ClaimIdle(ctx, r.Client, StreamKey, r.group(), r.Consumer, 30*time.Minute); err != nil {
			return nil, err
		}
		r.started = true
	}
	start := ">"
	if pending {
		start = "0"
	}
	res, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: r.group(), Consumer: r.Consumer, Streams: []string{StreamKey, start}, Count: 100, Block: -1,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []redis.XMessage
	for _, st := range res {
		out = append(out, st.Messages...)
	}
	return out, nil
}

func (r *RedisStream) Ack(ctx context.Context, id string) error {
	return r.Client.XAck(ctx, StreamKey, r.group(), id).Err()
}

// RedisState 是已同步状态的哈希（bf:connector:<source_id>:state）。
type RedisState struct {
	Client redis.Cmdable
	Key    string
}

func (r *RedisState) Forget(ctx context.Context, docIDs []string) error {
	for i := 0; i < len(docIDs); i += 1000 {
		if err := r.Client.HDel(ctx, r.Key, docIDs[i:min(i+1000, len(docIDs))]...).Err(); err != nil {
			return err
		}
	}
	return nil
}
