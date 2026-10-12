package access

import (
	"context"
	"crypto/ecdsa"

	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/denyrule"
)

// StreamChannel 把规则用备用通道的签名密钥签名后追加到事件流的独立主题（denyrule.Stream）。
// 主题不裁剪：规则只增不减，retrieval 每次启动从头读一遍。
type StreamChannel struct {
	Redis redis.Cmdable
	Key   *ecdsa.PrivateKey
}

func (c *StreamChannel) Publish(ctx context.Context, d Denial) error {
	signed, err := denyrule.Sign(c.Key, denyrule.Rule{
		ID: d.ID, Scope: d.Scope, Target: d.Target, Reason: d.Reason, IssuedBy: d.CreatedBy, IssuedAt: d.CreatedAt.UTC(),
	})
	if err != nil {
		return err
	}
	return c.Redis.XAdd(ctx, &redis.XAddArgs{Stream: denyrule.Stream, Values: map[string]any{"rule": signed}}).Err()
}
