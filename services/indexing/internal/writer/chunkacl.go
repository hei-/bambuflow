package writer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/events/v1"
)

// ChunkAclStream 是 ChunkAclChanged 的 Redis 流（retrieval 详设 3.5）：retrieval 的每个副本各自从最新处读，按文档清检索结果缓存。
// 只保留最近约 10 万条：副本重启时从最新处读，错过的事件对应的缓存最多 10 分钟后过期。
const ChunkAclStream = "retrieval:chunk-acl"

const chunkAclMaxLen = 100000

// ChunkAclNotifier 返回发 ChunkAclChanged 的 Notify。
func ChunkAclNotifier(rdb *redis.Client) func(ctx context.Context, docIDs []string, change string) error {
	return func(ctx context.Context, docIDs []string, change string) error {
		b := make([]byte, 12)
		_, _ = rand.Read(b)
		pb, err := proto.Marshal(&eventsv1.ChunkAclChanged{EventId: "evt_" + hex.EncodeToString(b), DocIds: docIDs, ChangeType: change,
			OccurredAt: timestamppb.New(time.Now())})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		return rdb.XAdd(ctx, &redis.XAddArgs{Stream: ChunkAclStream, MaxLen: chunkAclMaxLen, Approx: true,
			Values: map[string]any{"pb": pb}}).Err()
	}
}
