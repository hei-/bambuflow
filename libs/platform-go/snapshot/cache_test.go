package snapshot

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"os"
	"testing"
	"time"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

func TestCacheKeepsCurrentAndPrevious(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pub, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	now := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	v := &Verifier{Client: &keys{resp: &configv1.GetSigningKeysResponse{Keys: []*configv1.SigningKey{{KeyId: "k1", PublicKey: pub}}}},
		Now: func() time.Time { return now }}
	c := &Cache{Dir: t.TempDir(), Service: "gateway", Keys: v, Now: func() time.Time { return now }}
	ctx := context.Background()

	if _, _, err := c.Load(); !errors.Is(err, ErrNoCache) {
		t.Fatalf("空目录应为 ErrNoCache: %v", err)
	}
	save := func(version string) {
		t.Helper()
		s := signed(t, key, "k1", &configv1.GetSnapshotResponse{SnapshotVersion: version})
		if _, err := v.Open(ctx, s); err != nil {
			t.Fatal(err)
		}
		if err := c.Save(s); err != nil {
			t.Fatal(err)
		}
	}
	save("v1")
	now = now.Add(time.Minute)
	save("v1") // 同一份：只更新确认时间，不覆盖上一份
	save("v2")
	got, confirmed, err := c.Load()
	if err != nil || got.GetSnapshotVersion() != "v2" || !confirmed.Equal(now) {
		t.Fatalf("得到 %v %v %v", got, confirmed, err)
	}
	// 当前一份损坏：退回上一份
	if err := os.WriteFile(c.path("current"), []byte(`{"signed_payload":"AAAA"`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _, err := c.Load(); err != nil || got.GetSnapshotVersion() != "v1" {
		t.Fatalf("应退回上一份: %v %v", got, err)
	}
	// 公钥不认识（没验签过）的快照不写盘
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err := c.Save(signed(t, other, "k9", &configv1.GetSnapshotResponse{SnapshotVersion: "v3"})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("未知公钥应拒绝写盘: %v", err)
	}
}
