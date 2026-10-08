// Package webhook 投递平台事件（技术设计文档 13.7）：从事件流读取 qa/batch、indexing 等产生的事件，
// 按租户与事件名匹配订阅、为每个订阅记一条投递，再以 HMAC-SHA256 签名 POST 给接收方。
//
// 至少投递一次：消费事件流时先写投递记录再确认消息；同一事件对同一订阅只有一条记录（主键），重复消费不会重复投递。
// 失败按指数退避重试 24 小时；接收方按事件 ID 去重。事件体不含业务内容，只有事件 ID、类型、时间与对象 ID。
package webhook

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	eventsv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/events/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
)

// 请求头（contracts/openapi/v1/openapi.yaml 的 webhooks.platform_event）。
const (
	HeaderID        = "Bambuflow-Webhook-Id"
	HeaderTimestamp = "Bambuflow-Webhook-Timestamp"
	HeaderSignature = "Bambuflow-Webhook-Signature"
)

// EventTypes 是可以订阅的事件名：已经有产生方的事件。
var EventTypes = []string{outbox.BatchCompleted, outbox.BatchFailed, outbox.IndexVersionLive}

const (
	// RetryWindow 是重试的总时长（技术设计文档 13.7）。
	RetryWindow = 24 * time.Hour
	// 退避的起点与上限：30 秒起按 2 的幂增长，最长 1 小时一次，24 小时内约 30 次
	backoffBase = 30 * time.Second
	backoffMax  = time.Hour
	// MaxAttempts 是次数上限，与时间窗口二者先到为准
	MaxAttempts = 40
)

// Body 是发给接收方的请求体。
type Body struct {
	EventID    string `json:"event_id"`
	Type       string `json:"type"`
	OccurredAt string `json:"occurred_at"`
	ObjectID   string `json:"object_id"`
}

// Payload 生成事件的请求体。只取事件 ID、类型、时间与对象 ID：租户等内部字段不外发。
func Payload(evt *eventsv1.PlatformEvent) ([]byte, error) {
	return json.Marshal(Body{
		EventID: evt.GetEventId(), Type: evt.GetType(), ObjectID: evt.GetObjectId(),
		OccurredAt: evt.GetOccurredAt().AsTime().UTC().Format(time.RFC3339),
	})
}

// Signature 计算签名头的值：每个密钥一个 v1=<hex>，以逗号分隔。密钥轮换期间新旧两个都签，接收方任一匹配即可。
// 签名内容是“时间戳 + '.' + 原始请求体”，时间戳参与签名，接收方据此拒绝重放的旧请求。
func Signature(secrets []string, timestamp int64, body []byte) string {
	msg := append([]byte(strconv.FormatInt(timestamp, 10)+"."), body...)
	parts := make([]string, 0, len(secrets))
	for _, s := range secrets {
		mac := hmac.New(sha256.New, []byte(s))
		mac.Write(msg)
		parts = append(parts, "v1="+hex.EncodeToString(mac.Sum(nil)))
	}
	return strings.Join(parts, ",")
}

// NextAttempt 返回第 attempts 次失败后的下一次投递时间；超出重试窗口或次数上限时返回 false（不再重试）。
func NextAttempt(windowStart time.Time, attempts int, now time.Time) (time.Time, bool) {
	if attempts >= MaxAttempts {
		return time.Time{}, false
	}
	d := backoffBase
	for i := 1; i < attempts && d < backoffMax; i++ {
		d *= 2
	}
	next := now.Add(min(d, backoffMax))
	if next.After(windowStart.Add(RetryWindow)) {
		return time.Time{}, false
	}
	return next, true
}

// Matches：订阅是否要收到这个事件。事件不带租户时（共享索引的新版本上线等）发给所有租户的订阅。
func Matches(subTenant string, subTypes []string, evt *eventsv1.PlatformEvent) bool {
	return (evt.GetTenant() == "" || evt.GetTenant() == subTenant) && slices.Contains(subTypes, evt.GetType())
}

// ValidURL 校验回调地址：须为 https，开发环境可放开 http。不允许带用户信息（凭据不能出现在地址里）。
func ValidURL(raw string, allowHTTP bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("回调地址不合法")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !allowHTTP) {
		return errors.New("回调地址须为 https")
	}
	return nil
}

// ValidTypes 校验订阅的事件名：非空、不重复、都是可订阅的事件。
func ValidTypes(types []string) error {
	if len(types) == 0 {
		return errors.New("至少订阅一个事件")
	}
	seen := map[string]bool{}
	for _, t := range types {
		if !slices.Contains(EventTypes, t) || seen[t] {
			return errors.New("事件名未知或重复: " + t)
		}
		seen[t] = true
	}
	return nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// NewSubscriptionID 生成订阅 ID（whk_ 前缀）。
func NewSubscriptionID() string { return "whk_" + randomHex(12) }

// NewSecret 生成签名密钥（whsec_ 前缀，256 位随机数）。
func NewSecret() string { return "whsec_" + randomHex(32) }
