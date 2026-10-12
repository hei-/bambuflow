// Package snapshot 校验 governance 下发的配置快照（governance 详设 7.1、8.1、9.2）：
// 只信任签名，不信任传输通道。验签通过后从 signed_payload 解析出快照本体使用，不读外层的同名字段。
// 公钥经 ConfigService.GetSigningKeys 取得并缓存；遇到不认识的 key_id（密钥轮换）时最多每 30 秒重新拉一次。
// 加载成功的快照连同签名与公钥写本地磁盘（cache.go），governance 不可用时实例重启仍能用上一份。
package snapshot

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// ErrInvalid：快照没有签名、签名不对或公钥未知。
var ErrInvalid = errors.New("快照签名无效")

// KeyClient 是取公钥的接口（ConfigServiceClient 满足它）。
type KeyClient interface {
	GetSigningKeys(ctx context.Context, in *configv1.GetSigningKeysRequest, opts ...grpc.CallOption) (*configv1.GetSigningKeysResponse, error)
}

type Verifier struct {
	Client KeyClient
	Now    func() time.Time

	mu      sync.Mutex
	keys    map[string]key
	fetched time.Time
}

const refetchEvery = 30 * time.Second

type key struct {
	pub *ecdsa.PublicKey
	der []byte
}

// Open 验签并返回快照本体。
func (v *Verifier) Open(ctx context.Context, resp *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
	if len(resp.GetSignedPayload()) == 0 || len(resp.GetSignature()) == 0 {
		return nil, ErrInvalid
	}
	k, err := v.key(ctx, resp.GetSigningKeyId())
	if err != nil {
		return nil, err
	}
	return verify(k.pub, resp)
}

// PublicKey 返回已缓存的公钥（DER），写磁盘缓存时与快照一起保存；不认识时为 nil。
func (v *Verifier) PublicKey(id string) []byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.keys[id].der
}

func verify(pub *ecdsa.PublicKey, resp *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
	digest := sha256.Sum256(resp.GetSignedPayload())
	if !ecdsa.VerifyASN1(pub, digest[:], resp.GetSignature()) {
		return nil, ErrInvalid
	}
	out := &configv1.GetSnapshotResponse{}
	if err := proto.Unmarshal(resp.GetSignedPayload(), out); err != nil {
		return nil, ErrInvalid
	}
	return out, nil
}

func parseKey(der []byte) (*ecdsa.PublicKey, bool) {
	pub, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, false
	}
	ec, ok := pub.(*ecdsa.PublicKey)
	return ec, ok
}

func (v *Verifier) key(ctx context.Context, id string) (key, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if k, ok := v.keys[id]; ok {
		return k, nil
	}
	if !v.fetched.IsZero() && v.Now().Sub(v.fetched) < refetchEvery {
		return key{}, ErrInvalid
	}
	resp, err := v.Client.GetSigningKeys(ctx, &configv1.GetSigningKeysRequest{})
	if err != nil {
		return key{}, err
	}
	v.fetched = v.Now()
	v.keys = map[string]key{}
	for _, k := range resp.GetKeys() {
		if pub, ok := parseKey(k.GetPublicKey()); ok {
			v.keys[k.GetKeyId()] = key{pub: pub, der: k.GetPublicKey()}
		}
	}
	if k, ok := v.keys[id]; ok {
		return k, nil
	}
	return key{}, ErrInvalid
}
