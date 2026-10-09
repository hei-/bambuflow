package pii

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakeKeys struct {
	key   []byte
	calls int
	err   error
}

func (f *fakeKeys) Unwrap(_ context.Context, keyID, wrapped string) ([]byte, error) {
	f.calls++
	if f.err != nil || keyID != "bambuflow-pii" || wrapped != "vault:v1:x" {
		return nil, errors.New("不认识的密钥")
	}
	return f.key, nil
}

// seal 与 processing 的 pii.DocumentKey.seal 相同：12 字节随机数 + AES-256-GCM 密文，附加数据为 chunk_id
func seal(t *testing.T, key []byte, chunkID string, spans [][3]any) []byte {
	t.Helper()
	plain, _ := json.Marshal(spans)
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, 12)
	_, _ = rand.Read(nonce)
	return append(nonce, gcm.Seal(nil, nonce, plain, []byte(chunkID))...)
}

func TestRestoreSplicesOriginalsByRunePosition(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	keys := &fakeKeys{key: key}
	r := &Restorer{Keys: keys, TTL: time.Minute, Max: 10, Now: time.Now}
	// 位置按 Unicode 字符计：前面的中文不能让位置错开
	text := "客户手机 138****5678，邮箱 z********@corp.example。"
	ct := seal(t, key, "c1", [][3]any{{5, 16, "13812345678"}, {20, 29, "zhang.san"}})
	got, err := r.Restore(context.Background(), "c1", text, ct, "vault:v1:x", "bambuflow-pii")
	if err != nil || got != "客户手机 13812345678，邮箱 zhang.san@corp.example。" {
		t.Fatalf("得到 %q %v", got, err)
	}
	// 同一数据密钥第二次走缓存
	if _, err := r.Restore(context.Background(), "c1", text, ct, "vault:v1:x", "bambuflow-pii"); err != nil || keys.calls != 1 {
		t.Fatalf("应命中缓存: calls=%d %v", keys.calls, err)
	}
	// 密文换到别的块上解不开（附加数据是 chunk_id）
	if _, err := r.Restore(context.Background(), "c2", text, ct, "vault:v1:x", "bambuflow-pii"); err == nil {
		t.Fatal("别的块不应解得开")
	}
	// 位置越界视为密文损坏
	bad := seal(t, key, "c1", [][3]any{{5, 999, "x"}})
	if _, err := r.Restore(context.Background(), "c1", text, bad, "vault:v1:x", "bambuflow-pii"); !errors.Is(err, ErrMalformed) {
		t.Fatalf("得到 %v", err)
	}
}

func TestRestoreFailsWhenKeyServiceDown(t *testing.T) {
	r := &Restorer{Keys: &fakeKeys{err: errors.New("down")}, TTL: time.Minute, Max: 10, Now: time.Now}
	if _, err := r.Restore(context.Background(), "c1", "x", make([]byte, 40), "vault:v1:x", "bambuflow-pii"); err == nil {
		t.Fatal("密钥服务不可用时应报错，由调用方退回掩码")
	}
}
