package denials

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/denyrule"
)

func rule(id, scope, target string) denyrule.Rule {
	return denyrule.Rule{ID: id, Scope: scope, Target: target, Reason: "工单 SEC-1", IssuedBy: "ivan", IssuedAt: time.Now().UTC()}
}

// 两条备用通道：事件流与规则文件都只收验签通过的规则；伪造的丢弃。
func TestChannelsAcceptOnlySignedRules(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	forger, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	sign := func(k *ecdsa.PrivateKey, r denyrule.Rule) []byte {
		b, err := denyrule.Sign(k, r)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, b := range [][]byte{sign(key, rule("den_1", "source", "src_wiki")), sign(forger, rule("den_2", "tenant", "credit"))} {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: denyrule.Stream, Values: map[string]any{"rule": b}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "doc.json"), sign(key, rule("den_3", "document", "loan")), 0o600); err != nil {
		t.Fatal(err)
	}
	set := NewSet()
	c := &Channels{Set: set, Redis: rdb, Dir: dir, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		PublicKey: func(context.Context) (*ecdsa.PublicKey, error) { return &key.PublicKey, nil }}
	go c.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for !set.Source("src_wiki") || !set.Doc("loan") {
		if time.Now().After(deadline) {
			t.Fatal("两条通道的规则都应生效")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if set.Tenant("credit") {
		t.Fatal("伪造签名的规则不应生效")
	}
	// 启动后追加的条目同样生效
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: denyrule.Stream, Values: map[string]any{"rule": sign(key, rule("den_4", "tenant", "risk"))}}).Err(); err != nil {
		t.Fatal(err)
	}
	for !set.Tenant("risk") {
		if time.Now().After(deadline.Add(3 * time.Second)) {
			t.Fatal("新追加的规则应生效")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
