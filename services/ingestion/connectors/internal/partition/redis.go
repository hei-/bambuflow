package partition

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisState 是已发出的分区授权，存在 bf:connector:<数据源>:partitions。
type RedisState struct {
	Client   redis.Cmdable
	SourceID string
}

func (r RedisState) key() string { return "bf:connector:" + r.SourceID + ":partitions" }

func (r RedisState) All(ctx context.Context) (map[string]string, error) {
	return r.Client.HGetAll(ctx, r.key()).Result()
}

func (r RedisState) Set(ctx context.Context, partition, digest string) error {
	return r.Client.HSet(ctx, r.key(), partition, digest).Err()
}

func (r RedisState) Delete(ctx context.Context, partition string) error {
	return r.Client.HDel(ctx, r.key(), partition).Err()
}
