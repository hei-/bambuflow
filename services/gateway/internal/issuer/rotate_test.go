package issuer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hei-/bambuflow/libs/platform-go/vault"
)

type memKV struct {
	data     map[string]string
	version  int
	writeErr error
	// conflictOnce 模拟别的副本抢先写了一次
	conflictOnce func(*memKV)
}

func (m *memKV) ReadKVVersion(context.Context, string, string) (map[string]string, int, error) {
	return maps.Clone(m.data), m.version, nil
}

func (m *memKV) WriteKVCAS(_ context.Context, _, _ string, data map[string]string, version int) error {
	if m.conflictOnce != nil {
		f := m.conflictOnce
		m.conflictOnce = nil
		f(m)
	}
	if m.writeErr != nil {
		return m.writeErr
	}
	if version != m.version {
		return vault.ErrConflict
	}
	m.data, m.version = maps.Clone(data), m.version+1
	return nil
}

func jwksKIDs(t *testing.T, i *Issuer) []string {
	t.Helper()
	rec := httptest.NewRecorder()
	i.JWKS(rec, httptest.NewRequestWithContext(context.Background(), "GET", "/internal/jwks.json", nil))
	var body struct {
		Keys []struct {
			KID string `json:"kid"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, k := range body.Keys {
		out = append(out, k.KID)
	}
	return out
}

func newRotator(t *testing.T, kv *memKV, now *time.Time) *Rotator {
	t.Helper()
	return &Rotator{KV: kv, Mount: "secret", Path: "gateway/principal-signing", Issuer: Empty(),
		Every: 30 * 24 * time.Hour, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return *now }}
}

func seed(t *testing.T, created time.Time) *memKV {
	t.Helper()
	key, err := NewKeyPEM()
	if err != nil {
		t.Fatal(err)
	}
	return &memKV{data: map[string]string{FieldKey: key, FieldCreatedAt: created.Format(time.RFC3339)}, version: 1}
}

// 轮换分两步：先发布下一把（还用旧的签名），15 分钟后启用；旧的留在 JWKS 里，用它签的批量委托照样能验。
func TestRotationPublishesBeforeSigningAndKeepsThePreviousKey(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	kv := seed(t, now)
	r := newRotator(t, kv, &now)
	ctx := context.Background()
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	first := r.Issuer.KID()
	if got := jwksKIDs(t, r.Issuer); len(got) != 1 || got[0] != first || kv.version != 1 {
		t.Fatalf("未到期不应轮换: %v, version %d", got, kv.version)
	}
	if got := r.Remaining(); got != 30*24*time.Hour {
		t.Fatalf("剩余有效期应为一个轮换周期: %s", got)
	}
	d := delegation(now)
	d.Expires = now.Add(30*24*time.Hour + 2*time.Hour)
	r.Issuer.now = func() time.Time { return now.Add(30*24*time.Hour - time.Hour) }
	oldDelegation, err := r.Issuer.IssueDelegation(d)
	if err != nil {
		t.Fatal(err)
	}

	now = now.Add(30 * 24 * time.Hour)
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if kv.data[FieldNext] == "" || r.Issuer.KID() != first {
		t.Fatal("到期后应先发布下一把，仍用当前密钥签名")
	}
	if got := jwksKIDs(t, r.Issuer); len(got) != 2 {
		t.Fatalf("发布期间 JWKS 应有新旧两把: %v", got)
	}
	// 下一把已发布：按它的启用时间算，不报临近到期
	if got := r.Remaining(); got != 30*24*time.Hour+RotationLead {
		t.Fatalf("发布后剩余有效期应从下一把的启用时间算起: %s", got)
	}

	now = now.Add(RotationLead)
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	second := r.Issuer.KID()
	if second == first || kv.data[FieldNext] != "" || kv.data[FieldPrevious] == "" {
		t.Fatal("启用后应改用下一把签名，旧的降为上一把")
	}
	if got := jwksKIDs(t, r.Issuer); len(got) != 2 || got[0] != second || got[1] != first {
		t.Fatalf("JWKS 应发布当前与上一把: %v", got)
	}
	r.Issuer.now = func() time.Time { return now }
	if _, err := r.Issuer.VerifyDelegation(oldDelegation, "bat_01"); err != nil {
		t.Fatalf("旧密钥签的委托在轮换后应仍有效: %v", err)
	}
}

// 多个副本同时到期：只有一个写成，其他副本重读后用它写的那一把。
func TestConcurrentRotationKeepsTheWinnersKey(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	kv := seed(t, now.Add(-31*24*time.Hour))
	var winner string
	kv.conflictOnce = func(m *memKV) {
		key, _ := NewKeyPEM()
		winner = key
		m.data[FieldNext], m.data[FieldNextAt] = key, now.Add(RotationLead).Format(time.RFC3339)
		m.version++
	}
	r := newRotator(t, kv, &now)
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if kv.data[FieldNext] != winner || kv.version != 2 {
		t.Fatal("冲突后不应覆盖别的副本发布的下一把")
	}
	w, err := parsePEM(winner)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Issuer.publicKey(w.kid); !ok {
		t.Fatal("应发布别的副本写的下一把")
	}
}

// 写不回 Vault（比如策略只给了读权限）时照旧签发，不因轮换失败而停服。
func TestRotationWriteFailureKeepsSigning(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	kv := seed(t, time.Time{})
	delete(kv.data, FieldCreatedAt)
	kv.writeErr = errors.New("vault: 写入返回 403")
	r := newRotator(t, kv, &now)
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := jwksKIDs(t, r.Issuer); len(got) != 1 {
		t.Fatalf("没写成就不应发布只在内存里的密钥: %v", got)
	}
	if _, _, err := r.Issuer.Issue(Subject{Username: "alice", Tenant: "credit"}); err != nil {
		t.Fatal(err)
	}
	// 一直轮换不了：剩余有效期为负，由告警发现
	if r.Remaining() >= 0 {
		t.Fatalf("轮换写不回时剩余有效期应已过期: %s", r.Remaining())
	}
}

func TestRotationPeriodMustCoverDelegations(t *testing.T) {
	r := &Rotator{Every: 24 * time.Hour}
	if err := r.Validate(); err == nil {
		t.Fatal("短于委托有效期的轮换周期应拒绝")
	}
}
