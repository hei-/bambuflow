package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSealBindsObjectIdentity(t *testing.T) {
	key, _ := newKey()
	plain := []byte(`{"chunks":[{"text":"授信审批办法第三条"}]}`)
	sealed, err := seal(key, plain, docAAD("loan-policy", 3))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("授信")) {
		t.Fatal("密文里不应有明文")
	}
	got, err := open(key, sealed, docAAD("loan-policy", 3))
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("往返: %s %v", got, err)
	}
	// 换了位置（别的文档或版本）、换了密钥（密钥已销毁后另建的）都解不开
	if _, err := open(key, sealed, docAAD("loan-policy", 4)); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("换版本应解不开: %v", err)
	}
	other, _ := newKey()
	if _, err := open(other, sealed, docAAD("loan-policy", 3)); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("换密钥应解不开: %v", err)
	}
}

func TestKeysDoNotRevealDocIDs(t *testing.T) {
	k := docKey("src_hr:薪酬制度", 7)
	if strings.Contains(k, "薪酬") || !strings.HasPrefix(k, "docs/") || !strings.HasSuffix(k, "/7") {
		t.Fatalf("对象键: %s", k)
	}
	if s := shardOf("src_hr:薪酬制度"); s < 0 || s > 255 {
		t.Fatalf("分片越界: %d", s)
	}
	if keyringKey(10) != "keyring/0a.json" {
		t.Fatalf("密钥环分片键: %s", keyringKey(10))
	}
}

func TestSnapshotRetention(t *testing.T) {
	keys := []string{
		"snapshots/20261012T080000Z/manifest.json", "snapshots/20261012T080000Z/part-0000",
		"snapshots/20261013T090000Z/manifest.json",
		// 没写完 manifest 的不算
		"snapshots/20261014T090000Z/part-0000",
		"snapshots/20261014T080000Z/manifest.json",
	}
	ids := snapshotsIn(keys)
	if !slices.Equal(ids, []string{"20261012T080000Z", "20261013T090000Z", "20261014T080000Z"}) {
		t.Fatalf("快照: %v", ids)
	}
	now := time.Date(2026, 10, 14, 9, 30, 0, 0, time.UTC)
	drop, oldest := expired(ids, now)
	if !slices.Equal(drop, []string{"20261012T080000Z"}) || !oldest.Equal(time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("过期: %v %v", drop, oldest)
	}
	// 只剩一个快照时再旧也保留
	if drop, _ := expired([]string{"20260101T000000Z"}, now); len(drop) != 0 {
		t.Fatalf("最新的快照总是保留: %v", drop)
	}
}

// fakeTransit 模拟 Vault transit 的批量接口：密文就是 "vault:v1:" 加明文的 base64
func fakeTransit(t *testing.T) (*Transit, *int) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("X-Vault-Token") != "tok" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		var in struct {
			BatchInput []map[string]string `json:"batch_input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		// 与真实的 Vault 一样，每条结果带数字字段 key_version
		out := make([]map[string]any, len(in.BatchInput))
		for i, item := range in.BatchInput {
			if strings.HasSuffix(r.URL.Path, "/transit/encrypt/bambuflow-backup") {
				out[i] = map[string]any{"ciphertext": "vault:v1:" + item["plaintext"], "key_version": 1}
			} else {
				out[i] = map[string]any{"plaintext": strings.TrimPrefix(item["ciphertext"], "vault:v1:"), "key_version": 1}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"batch_results": out}})
	}))
	t.Cleanup(srv.Close)
	return &Transit{Addr: srv.URL, Key: "bambuflow-backup", Token: "tok"}, &calls
}

func TestTransitBatchesWrapAndUnwrap(t *testing.T) {
	tr, calls := fakeTransit(t)
	keys := make([][]byte, batchSize+3)
	for i := range keys {
		keys[i], _ = newKey()
	}
	ctx := context.Background()
	wrapped, err := tr.Wrap(ctx, keys)
	if err != nil || len(wrapped) != len(keys) || *calls != 2 {
		t.Fatalf("按批包裹: %d 条 %d 次 %v", len(wrapped), *calls, err)
	}
	got, err := tr.Unwrap(ctx, wrapped)
	if err != nil || !bytes.Equal(got[batchSize+2], keys[batchSize+2]) {
		t.Fatalf("解开: %v", err)
	}
	tr.Token = "wrong"
	if _, err := tr.Unwrap(ctx, wrapped[:1]); err == nil {
		t.Fatal("密钥服务拒绝时应报错")
	}
}
