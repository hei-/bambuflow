package vault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func fakeVault(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "t0k" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path != "/v1/secret/data/gateway/principal-signing" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"data":{"kid":"k1","private_key":"-----BEGIN..."},"metadata":{"version":3}}}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestReadKV(t *testing.T) {
	srv := fakeVault(t)
	c := New(srv.URL, func() (string, error) { return "t0k", nil })
	got, err := c.ReadKV(context.Background(), "secret", "gateway/principal-signing")
	if err != nil || got["kid"] != "k1" {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := c.ReadKV(context.Background(), "secret", "gateway/other"); !errors.Is(err, ErrNotFound) {
		t.Errorf("不存在的路径应返回 ErrNotFound，得到 %v", err)
	}
	bad := New(srv.URL, func() (string, error) { return "wrong", nil })
	if _, err := bad.ReadKV(context.Background(), "secret", "gateway/principal-signing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("无权读取应返回 ErrNotFound，得到 %v", err)
	}
}

func TestFromEnvReadsTokenFileEachTime(t *testing.T) {
	srv := fakeVault(t)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VAULT_ADDR", srv.URL)
	t.Setenv("VAULT_TOKEN_FILE", file)
	t.Setenv("VAULT_TOKEN", "")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadKV(context.Background(), "secret", "gateway/principal-signing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("旧令牌应被拒绝，得到 %v", err)
	}
	// Vault Agent 续期后改写令牌文件，下一次读取应使用新令牌
	if err := os.WriteFile(file, []byte("t0k\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadKV(context.Background(), "secret", "gateway/principal-signing"); err != nil {
		t.Fatalf("应读到新令牌: %v", err)
	}
}

func TestFromEnvRequiresConfig(t *testing.T) {
	t.Setenv("VAULT_ADDR", "")
	if _, err := FromEnv(); err == nil {
		t.Error("没有 VAULT_ADDR 应报错")
	}
}

func TestWriteKV(t *testing.T) {
	var got map[string]map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "t0k" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/secret/data/webhooks/whk_1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"data":{"version":1}}`))
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, func() (string, error) { return "t0k", nil })
	if err := c.WriteKV(context.Background(), "secret", "webhooks/whk_1", map[string]string{"current": "s1"}); err != nil {
		t.Fatal(err)
	}
	if got["data"]["current"] != "s1" {
		t.Fatalf("写入的内容不对: %v", got)
	}
	bad := New(srv.URL, func() (string, error) { return "wrong", nil })
	if err := bad.WriteKV(context.Background(), "secret", "webhooks/whk_1", map[string]string{"current": "s1"}); err == nil {
		t.Fatal("无权写入应报错")
	}
}
