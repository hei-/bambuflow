package issuer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync/atomic"
	"time"

	"github.com/hei-/bambuflow/libs/platform-go/vault"
)

// 签名密钥在 Vault KV 里的字段（gateway 详设 3.2，部署配置清单 P10）。私钥都是 PKCS#8 PEM 格式的 P-256 密钥，时间是 RFC 3339。
const (
	// FieldKey 是当前的密钥
	FieldKey = "private_key"
	// FieldCreatedAt 是当前密钥开始签名的时间，轮换周期从它算起；缺失时视为已到期
	FieldCreatedAt = "created_at"
	// FieldNext、FieldNextAt 是已发布、到时间才启用的下一把密钥
	FieldNext   = "next_private_key"
	FieldNextAt = "next_not_before"
	// FieldPrevious 是上一把密钥：不再签名，留在 JWKS 里到下一次轮换
	FieldPrevious = "previous_private_key"
)

// RotationLead 是下一把密钥从发布到启用的间隔：要长于下游缓存 JWKS 的时间（principal.JWKS 最长 10 分钟）
// 加上各副本重读 Vault 的间隔，启用时下游都已经拿到它的公钥。
const RotationLead = 15 * time.Minute

// KV 是 Vault KV v2 的读写（libs/platform-go/vault.Client）。
type KV interface {
	ReadKVVersion(ctx context.Context, mount, path string) (map[string]string, int, error)
	WriteKVCAS(ctx context.Context, mount, path string, data map[string]string, version int) error
}

// Rotator 定期从 Vault 重读签名密钥装进签发器，到期时轮换。轮换分两步，都由某个 gateway 副本用 check-and-set 写回，
// 多副本同时到期也只有一个写成：① 生成下一把并发布（JWKS 里新旧两把共存）；② RotationLead 之后启用下一把，
// 当前一把降为上一把。生成的私钥只写进 Vault，不落盘、不进日志。
type Rotator struct {
	KV          KV
	Mount, Path string
	Issuer      *Issuer
	// Every 是轮换周期（部署配置清单 P10，默认 30 天）
	Every time.Duration
	Log   *slog.Logger
	Now   func() time.Time

	// 当前签名密钥按轮换周期的到期时间（Unix 秒），Sync 成功时更新；轮换一直写不回 Vault 时它会过去，由告警发现
	expires atomic.Int64
}

// Remaining 是当前签名密钥离按轮换周期到期还有多久（gateway 详设 11.1“签名密钥剩余有效期”）；
// 正常情况下自动轮换，不会低于 RotationLead；还没 Sync 成功过时为 0。
func (r *Rotator) Remaining() time.Duration {
	exp := r.expires.Load()
	if exp == 0 {
		return 0
	}
	return time.Unix(exp, 0).Sub(r.Now())
}

// Validate 检查轮换周期：上一把密钥只留到下一次轮换，周期短于批量任务委托的最长有效期时，执行中的批次会因验不了签而失败。
func (r *Rotator) Validate() error {
	if r.Every < MaxDelegationLifetime {
		return fmt.Errorf("签名密钥轮换周期 %s 短于批量任务委托的最长有效期 %s", r.Every, MaxDelegationLifetime)
	}
	return nil
}

// Sync 读 Vault 里的密钥，需要时推进轮换，再装进签发器。读不到时返回错误，签发器保持原样。
// 写回失败（并发冲突以外，如无写权限）只记错误：照旧用读到的密钥，不会因为轮换不了而停止签发。
func (r *Rotator) Sync(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		data, version, err := r.KV.ReadKVVersion(ctx, r.Mount, r.Path)
		if err != nil {
			return fmt.Errorf("读取 Principal 签名密钥: %w", err)
		}
		now := r.Now()
		next, step, err := advance(data, now, r.Every)
		if err != nil {
			return err
		}
		if step != "" {
			err := r.KV.WriteKVCAS(ctx, r.Mount, r.Path, next, version)
			switch {
			case errors.Is(err, vault.ErrConflict) && attempt == 0:
				// 别的副本刚写过：重读它写的
				continue
			case err != nil:
				r.Log.ErrorContext(ctx, "轮换 Principal 签名密钥失败，继续用现有密钥", slog.String("step", step), slog.Any("err", err))
				next = data
			default:
				r.Log.InfoContext(ctx, "Principal 签名密钥轮换", slog.String("step", step))
			}
		}
		if err := r.Issuer.load(next, now); err != nil {
			return err
		}
		// 已发布下一把时从它的启用时间算起（发布到启用的 15 分钟里不算临近到期）；
		// 缺少或无法解析的开始时间视为已到期（与 advance 一致）
		start, _ := time.Parse(time.RFC3339, next[FieldCreatedAt])
		if next[FieldNext] != "" {
			start, _ = time.Parse(time.RFC3339, next[FieldNextAt])
		}
		r.expires.Store(start.Add(r.Every).Unix())
		return nil
	}
}

// Run 每隔 every 执行一次 Sync，直到 ctx 结束。失败只记录：签发器保持上一次装载的密钥。
func (r *Rotator) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := r.Sync(ctx); err != nil {
			r.Log.ErrorContext(ctx, "重读 Principal 签名密钥失败", slog.Any("err", err))
		}
	}
}

// advance 返回推进一步之后的密钥与这一步的名字（publish 或 activate）；不需要推进时 step 为空。
func advance(data map[string]string, now time.Time, every time.Duration) (map[string]string, string, error) {
	if data[FieldKey] == "" {
		return nil, "", fmt.Errorf("签名密钥缺少 %s", FieldKey)
	}
	out := maps.Clone(data)
	if data[FieldNext] != "" {
		at, err := time.Parse(time.RFC3339, data[FieldNextAt])
		if err != nil {
			return nil, "", fmt.Errorf("%s 不合法: %w", FieldNextAt, err)
		}
		if now.Before(at) {
			return data, "", nil
		}
		out[FieldPrevious], out[FieldKey], out[FieldCreatedAt] = data[FieldKey], data[FieldNext], data[FieldNextAt]
		delete(out, FieldNext)
		delete(out, FieldNextAt)
		return out, "activate", nil
	}
	if created, err := time.Parse(time.RFC3339, data[FieldCreatedAt]); err == nil && now.Sub(created) < every {
		return data, "", nil
	}
	key, err := NewKeyPEM()
	if err != nil {
		return nil, "", err
	}
	out[FieldNext], out[FieldNextAt] = key, now.Add(RotationLead).UTC().Format(time.RFC3339)
	return out, "publish", nil
}

// load 按 Vault 里的字段装载密钥：下一把到了启用时间就用它签名（别的副本还没写回第 ② 步时也一致）。
func (i *Issuer) load(data map[string]string, now time.Time) error {
	cur, err := parsePEM(data[FieldKey])
	if err != nil {
		return fmt.Errorf("%s: %w", FieldKey, err)
	}
	set := &keySet{signing: cur, all: []signingKey{cur}}
	if data[FieldNext] != "" {
		next, err := parsePEM(data[FieldNext])
		if err != nil {
			return fmt.Errorf("%s: %w", FieldNext, err)
		}
		if at, err := time.Parse(time.RFC3339, data[FieldNextAt]); err == nil && !now.Before(at) {
			set.signing = next
		}
		set.all = append(set.all, next)
	}
	if data[FieldPrevious] != "" {
		prev, err := parsePEM(data[FieldPrevious])
		if err != nil {
			return fmt.Errorf("%s: %w", FieldPrevious, err)
		}
		set.all = append(set.all, prev)
	}
	i.keys.Store(set)
	return nil
}

// NewKeyPEM 生成一把 P-256 私钥（PKCS#8 PEM）。
func NewKeyPEM() (string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), nil
}
