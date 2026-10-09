// Package snapshot 校验 governance 下发的配置快照（governance 详设 7.1、8.1、9.2）：
// 只信任签名，不信任传输通道。验签通过后从 signed_payload 解析出快照本体使用，不读外层的同名字段。
// 公钥经 ConfigService.GetSigningKeys 取得并缓存；遇到不认识的 key_id（密钥轮换）时最多每 30 秒重新拉一次。
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

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
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
	keys    map[string]*ecdsa.PublicKey
	fetched time.Time
}

const refetchEvery = 30 * time.Second

// Open 验签并返回快照本体。
func (v *Verifier) Open(ctx context.Context, resp *configv1.GetSnapshotResponse) (*configv1.GetSnapshotResponse, error) {
	if len(resp.GetSignedPayload()) == 0 || len(resp.GetSignature()) == 0 {
		return nil, ErrInvalid
	}
	key, err := v.key(ctx, resp.GetSigningKeyId())
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(resp.GetSignedPayload())
	if !ecdsa.VerifyASN1(key, digest[:], resp.GetSignature()) {
		return nil, ErrInvalid
	}
	out := &configv1.GetSnapshotResponse{}
	if err := proto.Unmarshal(resp.GetSignedPayload(), out); err != nil {
		return nil, ErrInvalid
	}
	return out, nil
}

func (v *Verifier) key(ctx context.Context, id string) (*ecdsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if k, ok := v.keys[id]; ok {
		return k, nil
	}
	if !v.fetched.IsZero() && v.Now().Sub(v.fetched) < refetchEvery {
		return nil, ErrInvalid
	}
	resp, err := v.Client.GetSigningKeys(ctx, &configv1.GetSigningKeysRequest{})
	if err != nil {
		return nil, err
	}
	v.fetched = v.Now()
	v.keys = map[string]*ecdsa.PublicKey{}
	for _, k := range resp.GetKeys() {
		pub, err := x509.ParsePKIXPublicKey(k.GetPublicKey())
		if err != nil {
			continue
		}
		if ec, ok := pub.(*ecdsa.PublicKey); ok {
			v.keys[k.GetKeyId()] = ec
		}
	}
	if k, ok := v.keys[id]; ok {
		return k, nil
	}
	return nil, ErrInvalid
}
