package snapshot

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

type keys struct {
	resp  *configv1.GetSigningKeysResponse
	calls int
}

func (k *keys) GetSigningKeys(context.Context, *configv1.GetSigningKeysRequest, ...grpc.CallOption) (*configv1.GetSigningKeysResponse, error) {
	k.calls++
	return k.resp, nil
}

func signed(t *testing.T, key *ecdsa.PrivateKey, kid string, body *configv1.GetSnapshotResponse) *configv1.GetSnapshotResponse {
	t.Helper()
	payload, _ := proto.Marshal(body)
	d := sha256.Sum256(payload)
	sig, _ := ecdsa.SignASN1(rand.Reader, key, d[:])
	// 外层字段故意写成别的值：使用方只认签名载荷
	return &configv1.GetSnapshotResponse{SnapshotVersion: "forged", SignedPayload: payload, Signature: sig, SigningKeyId: kid}
}

func TestOpenVerifiesAndUsesSignedPayloadOnly(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pub, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	k := &keys{resp: &configv1.GetSigningKeysResponse{Keys: []*configv1.SigningKey{{KeyId: "k1", PublicKey: pub}}}}
	now := time.Unix(0, 0)
	v := &Verifier{Client: k, Now: func() time.Time { return now }}
	ctx := context.Background()

	got, err := v.Open(ctx, signed(t, key, "k1", &configv1.GetSnapshotResponse{SnapshotVersion: "v1"}))
	if err != nil || got.GetSnapshotVersion() != "v1" {
		t.Fatalf("得到 %v %v", got, err)
	}
	// 载荷被改：验签失败
	bad := signed(t, key, "k1", &configv1.GetSnapshotResponse{SnapshotVersion: "v1"})
	bad.SignedPayload = append(bad.SignedPayload, 0x08, 0x01)
	if _, err := v.Open(ctx, bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("改过的载荷应拒绝: %v", err)
	}
	// 别的私钥签的：拒绝
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if _, err := v.Open(ctx, signed(t, other, "k1", &configv1.GetSnapshotResponse{SnapshotVersion: "v2"})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("签名不对应拒绝: %v", err)
	}
	// 没有签名：拒绝
	if _, err := v.Open(ctx, &configv1.GetSnapshotResponse{SnapshotVersion: "v1"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("没签名应拒绝: %v", err)
	}
	// 不认识的 key_id：30 秒内不重复拉公钥
	if _, err := v.Open(ctx, signed(t, key, "k2", &configv1.GetSnapshotResponse{SnapshotVersion: "v2"})); !errors.Is(err, ErrInvalid) || k.calls != 1 {
		t.Fatalf("未知 key_id: %v calls=%d", err, k.calls)
	}
	now = now.Add(31 * time.Second)
	k.resp.Keys = append(k.resp.Keys, &configv1.SigningKey{KeyId: "k2", PublicKey: pub})
	if _, err := v.Open(ctx, signed(t, key, "k2", &configv1.GetSnapshotResponse{SnapshotVersion: "v2"})); err != nil || k.calls != 2 {
		t.Fatalf("轮换后应能验签: %v calls=%d", err, k.calls)
	}
}
