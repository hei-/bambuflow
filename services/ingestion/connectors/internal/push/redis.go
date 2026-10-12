package push

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// RedisState 是已推送的状态，与连接器同步共用 bf:connector:<数据源>:state。
type RedisState struct {
	Client redis.Cmdable
}

func stateKey(sourceID string) string { return "bf:connector:" + sourceID + ":state" }

func (r RedisState) Get(ctx context.Context, sourceID, docID string) (string, bool, error) {
	v, err := r.Client.HGet(ctx, stateKey(sourceID), docID).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	return v, err == nil, err
}

func (r RedisState) Set(ctx context.Context, sourceID, docID, fingerprint string) error {
	return r.Client.HSet(ctx, stateKey(sourceID), docID, fingerprint).Err()
}

func (r RedisState) Delete(ctx context.Context, sourceID, docID string) error {
	return r.Client.HDel(ctx, stateKey(sourceID), docID).Err()
}

func (r RedisState) Count(ctx context.Context, sourceID string) (int64, error) {
	return r.Client.HLen(ctx, stateKey(sourceID)).Result()
}
