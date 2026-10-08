package principal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	// 遇到未知 kid 时最多每隔这么久刷新一次，防止伪造 kid 的请求把 JWKS 端点打满。
	minRefreshInterval = 30 * time.Second
	// 缓存超过这个时间就在下次使用时刷新，使轮换下线的旧密钥及时失效。
	maxCacheAge = 10 * time.Minute
)

// JWKS 从 gateway 的内部 JWKS 端点获取验签公钥并缓存。gateway 轮换密钥时新旧两把同时发布，按 kid 都能找到。
type JWKS struct {
	url    string
	client *http.Client
	now    func() time.Time

	mu        sync.Mutex
	keys      map[string]*ecdsa.PublicKey
	fetchedAt time.Time
}

func NewJWKS(url string, client *http.Client) *JWKS {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &JWKS{url: url, client: client, now: time.Now}
}

// Key 返回 kid 对应的公钥。缓存里没有或缓存过期时刷新；刷新失败且缓存里没有该 kid 时返回错误（fail-closed）。
// 刷新失败但缓存里有该 kid 时继续使用缓存：这是公钥，短时间沿用不扩大权限，且避免 JWKS 端点抖动导致全部请求失败。
func (j *JWKS) Key(ctx context.Context, kid string) (any, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	key, ok := j.keys[kid]
	stale := j.now().Sub(j.fetchedAt) > maxCacheAge
	canRefresh := j.fetchedAt.IsZero() || j.now().Sub(j.fetchedAt) >= minRefreshInterval
	if (!ok || stale) && canRefresh {
		keys, err := j.fetch(ctx)
		j.fetchedAt = j.now()
		if err == nil {
			j.keys = keys
			key, ok = keys[kid]
		} else if !ok {
			return nil, fmt.Errorf("获取 JWKS 失败: %w", err)
		}
	}
	if !ok {
		return nil, fmt.Errorf("未知的 kid %q", kid)
	}
	return key, nil
}

type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (j *JWKS) fetch(ctx context.Context) (map[string]*ecdsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := j.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS 端点返回 %d", resp.StatusCode)
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, err
	}
	keys := map[string]*ecdsa.PublicKey{}
	for _, k := range set.Keys {
		// 只接受 P-256 签名公钥，其他类型的密钥忽略
		if k.Kty != "EC" || k.Crv != "P-256" || k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		pub, err := ecPublicKey(k.X, k.Y)
		if err != nil {
			return nil, fmt.Errorf("kid %q: %w", k.Kid, err)
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return nil, errors.New("JWKS 中没有可用的 P-256 公钥")
	}
	return keys, nil
}

func ecPublicKey(x, y string) (*ecdsa.PublicKey, error) {
	xb, err := base64.RawURLEncoding.DecodeString(x)
	if err != nil {
		return nil, err
	}
	yb, err := base64.RawURLEncoding.DecodeString(y)
	if err != nil {
		return nil, err
	}
	if len(xb) != 32 || len(yb) != 32 {
		return nil, errors.New("P-256 坐标长度应为 32 字节")
	}
	// 未压缩点格式 0x04 || X || Y；解析时会拒绝不在曲线上的点（防无效曲线攻击）
	return ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, xb...), yb...))
}
