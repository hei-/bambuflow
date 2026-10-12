// Package hook 接收源系统的变更通知（Webhook，入库管道详设第 5 节），验签后唤醒这个数据源的清单轮询（internal/poll）。
//
// 签名与平台对外的 Webhook 相同（contracts/openapi/v1/openapi.yaml 的 webhooks.platform_event）：
// Bambuflow-Webhook-Timestamp 是 Unix 秒，Bambuflow-Webhook-Signature 是一个或多个 v1=<hex>，
// 内容为 HMAC-SHA256(密钥, 时间戳 + "." + 原始请求体)。时间戳与当前时间相差超过 5 分钟的拒绝（防重放）；
// 密钥轮换期间可以配新旧两个，任一匹配即可。
//
// 通知内容不解析：源系统的通知可能丢、可能乱序，也可能被伪造得恰好验过签，变更一律以唤醒后的那一轮清单为准。
package hook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	HeaderTimestamp = "Bambuflow-Webhook-Timestamp"
	HeaderSignature = "Bambuflow-Webhook-Signature"
	// Tolerance：时间戳允许的偏差
	Tolerance = 5 * time.Minute
	// MaxBody：请求体上限；只用来验签
	MaxBody = 64 << 10
	// MinSecret：密钥最短长度
	MinSecret = 32
)

type Handler struct {
	// Secrets 是当前有效的密钥（轮换期间两个）
	Secrets []string
	// Wake 唤醒轮询
	Wake func(ctx context.Context) error
	Log  *slog.Logger
	Now  func() time.Time
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	if !h.verify(r.Header.Get(HeaderTimestamp), r.Header.Get(HeaderSignature), body) {
		// 不说明是时间戳还是签名不对
		h.Log.WarnContext(r.Context(), "Webhook 验签失败", slog.String("remote", r.RemoteAddr))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if err := h.Wake(r.Context()); err != nil {
		// 源系统会重试；即使一直失败，下一轮定时轮询也会发现这次变更
		h.Log.ErrorContext(r.Context(), "唤醒轮询失败", slog.Any("err", err))
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) verify(ts, sig string, body []byte) bool {
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	if d := h.Now().Sub(time.Unix(t, 0)); d > Tolerance || d < -Tolerance {
		return false
	}
	msg := append([]byte(ts+"."), body...)
	for _, part := range strings.Split(sig, ",") {
		got, ok := strings.CutPrefix(strings.TrimSpace(part), "v1=")
		if !ok {
			continue
		}
		raw, err := hex.DecodeString(got)
		if err != nil {
			continue
		}
		for _, s := range h.Secrets {
			mac := hmac.New(sha256.New, []byte(s))
			mac.Write(msg)
			if hmac.Equal(raw, mac.Sum(nil)) {
				return true
			}
		}
	}
	return false
}
