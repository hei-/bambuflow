package hook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

const (
	current  = "current-secret-0123456789abcdef0123"
	previous = "previous-secret-0123456789abcdef012"
)

func sign(secret string, ts int64, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10) + "." + body))
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestWebhookVerifiesAndWakes(t *testing.T) {
	now := time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)
	wakes := 0
	var wakeErr error
	h := &Handler{Secrets: []string{current, previous}, Log: quiet, Now: func() time.Time { return now },
		Wake: func(context.Context) error { wakes++; return wakeErr }}
	body := `{"event":"page.updated","page_id":"loan-policy"}`
	send := func(method string, ts int64, sig, body string) int {
		req := httptest.NewRequestWithContext(context.Background(), method, "/webhook", strings.NewReader(body))
		req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
		req.Header.Set(HeaderSignature, sig)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}
	ts := now.Unix()
	if c := send(http.MethodPost, ts, sign(current, ts, body), body); c != http.StatusAccepted || wakes != 1 {
		t.Fatalf("正确签名: %d %d", c, wakes)
	}
	// 轮换期间旧密钥签的也接受；签名头里有多个值时任一匹配即可
	if c := send(http.MethodPost, ts, "v1=00,"+sign(previous, ts, body), body); c != http.StatusAccepted {
		t.Fatalf("旧密钥: %d", c)
	}
	for name, c := range map[string]int{
		"签名不对":   send(http.MethodPost, ts, sign("another-secret-0123456789abcdef0123", ts, body), body),
		"内容被改":   send(http.MethodPost, ts, sign(current, ts, body), body+" "),
		"时间戳过旧":  send(http.MethodPost, ts-600, sign(current, ts-600, body), body),
		"时间戳在未来": send(http.MethodPost, ts+600, sign(current, ts+600, body), body),
		"没有签名":   send(http.MethodPost, ts, "", body),
	} {
		if c != http.StatusUnauthorized {
			t.Errorf("%s: %d", name, c)
		}
	}
	if c := send(http.MethodGet, ts, sign(current, ts, ""), ""); c != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d", c)
	}
	big := strings.Repeat("x", MaxBody+1)
	if c := send(http.MethodPost, ts, sign(current, ts, big), big); c != http.StatusRequestEntityTooLarge {
		t.Errorf("请求体过大: %d", c)
	}
	wakeErr = errors.New("redis 不可用")
	if c := send(http.MethodPost, ts, sign(current, ts, body), body); c != http.StatusServiceUnavailable {
		t.Errorf("唤醒失败时让源系统重试: %d", c)
	}
	if wakes != 3 {
		t.Errorf("验签失败的请求不应唤醒: %d", wakes)
	}
}
