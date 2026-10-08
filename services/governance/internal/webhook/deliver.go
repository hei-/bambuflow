package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DeliveryStore 是投递记录的读写（PG）。
type DeliveryStore interface {
	Claim(ctx context.Context, n int, lease time.Duration) ([]Delivery, error)
	Delivered(ctx context.Context, d Delivery, code int) error
	Failed(ctx context.Context, d Delivery, code int, msg string, next time.Time) error
}

// SecretReader 读取 Vault KV（libs/platform-go/vault）。
type SecretReader interface {
	ReadKV(ctx context.Context, mount, path string) (map[string]string, error)
}

const (
	// 单次投递的超时：接收方应尽快返回 2xx、把处理放到后台
	sendTimeout = 10 * time.Second
	// 租约覆盖一次投递并留出余量
	deliveryLease = time.Minute
)

// NewHTTPClient 返回投递用的 HTTP 客户端：不跟随重定向（回调地址只认登记的那一个，
// 不让接收方把带签名的请求转到别处），有总超时。
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout:       sendTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

type Deliverer struct {
	Store   DeliveryStore
	Secrets SecretReader
	// 签名密钥所在的 KV 引擎挂载点
	Mount string
	HTTP  *http.Client
	Log   *slog.Logger
	Now   func() time.Time
	// 每轮最多同时投递的条数
	Concurrency  int
	PollInterval time.Duration
}

// Run 循环投递直到 ctx 结束。
func (d *Deliverer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := d.Once(ctx)
		if err != nil && ctx.Err() == nil {
			d.Log.WarnContext(ctx, "领取 Webhook 投递失败", slog.Any("err", err))
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(d.PollInterval):
		}
	}
}

// Once 领取一轮到期的投递并发送，返回领取的条数。
func (d *Deliverer) Once(ctx context.Context) (int, error) {
	batch, err := d.Store.Claim(ctx, d.Concurrency, deliveryLease)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, del := range batch {
		wg.Go(func() { d.send(context.WithoutCancel(ctx), del) })
	}
	wg.Wait()
	return len(batch), nil
}

func (d *Deliverer) send(ctx context.Context, del Delivery) {
	log := d.Log.With(slog.String("event_id", del.EventID), slog.String("subscription_id", del.SubscriptionID), slog.Int("attempt", del.Attempts))
	if !del.Active {
		d.record(ctx, log, d.Store.Failed(ctx, del, 0, "订阅已停用", time.Time{}))
		return
	}
	code, err := d.post(ctx, del)
	if err == nil && code >= 200 && code < 300 {
		d.record(ctx, log, d.Store.Delivered(ctx, del, code))
		return
	}
	msg := "HTTP " + strconv.Itoa(code)
	if err != nil {
		// 只记错误类别，不记接收方返回的内容
		msg = err.Error()
	}
	next, retry := NextAttempt(del.WindowStart, del.Attempts, d.Now())
	if !retry {
		log.WarnContext(ctx, "Webhook 投递放弃", slog.String("last_error", msg))
	}
	d.record(ctx, log, d.Store.Failed(ctx, del, code, msg, next))
}

func (d *Deliverer) record(ctx context.Context, log *slog.Logger, err error) {
	if err != nil {
		// 没记下来：租约过期后会重投，接收方按事件 ID 去重
		log.ErrorContext(ctx, "写投递记录失败", slog.Any("err", err))
	}
}

var (
	errSecret = errors.New("读取签名密钥失败")
	errSend   = errors.New("连接失败或超时")
)

// post 签名并发送，返回 HTTP 状态码。
func (d *Deliverer) post(ctx context.Context, del Delivery) (int, error) {
	kv, err := d.Secrets.ReadKV(ctx, d.Mount, del.SecretPath)
	if err != nil || kv["current"] == "" {
		d.Log.ErrorContext(ctx, "读取 Webhook 签名密钥失败", slog.String("subscription_id", del.SubscriptionID), slog.Any("err", err))
		return 0, errSecret
	}
	secrets := []string{kv["current"]}
	if kv["previous"] != "" {
		secrets = append(secrets, kv["previous"])
	}
	ts := d.Now().Unix()
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, del.URL, bytes.NewReader(del.Payload))
	if err != nil {
		return 0, fmt.Errorf("回调地址不合法: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "bambuflow-webhook/1")
	req.Header.Set(HeaderID, del.EventID)
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, Signature(secrets, ts, del.Payload))
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return 0, errSend
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}
