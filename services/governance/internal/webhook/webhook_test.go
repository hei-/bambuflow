package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	eventsv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/events/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
)

var t0 = time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)

func quietLog() *slog.Logger { return logging.New(&bytes.Buffer{}, "t", slog.LevelInfo) }

// verify 按接收方的做法校验签名（openapi.yaml 的 webhooks.platform_event）。
func verify(secret, header, ts string, body []byte) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "."))
	mac.Write(body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	for _, part := range strings.Split(header, ",") {
		if hmac.Equal([]byte(part), []byte(want)) {
			return true
		}
	}
	return false
}

func TestSignatureCoversTimestampAndBody(t *testing.T) {
	body := []byte(`{"event_id":"evt_1"}`)
	h := Signature([]string{"new", "old"}, 1700000000, body)
	if !verify("new", h, "1700000000", body) || !verify("old", h, "1700000000", body) {
		t.Fatalf("轮换期间新旧密钥都应能校验: %s", h)
	}
	if verify("other", h, "1700000000", body) {
		t.Fatal("别的密钥不应通过")
	}
	if verify("new", h, "1700000001", body) {
		t.Fatal("改了时间戳不应通过")
	}
	if verify("new", h, "1700000000", []byte(`{"event_id":"evt_2"}`)) {
		t.Fatal("改了请求体不应通过")
	}
}

func TestNextAttemptBackoffWindowAndCap(t *testing.T) {
	now := t0
	var gaps []time.Duration
	attempts := 0
	for {
		attempts++
		next, ok := NextAttempt(t0, attempts, now)
		if !ok {
			break
		}
		gaps = append(gaps, next.Sub(now))
		now = next
	}
	if gaps[0] != 30*time.Second || gaps[1] != time.Minute || gaps[len(gaps)-1] != time.Hour {
		t.Fatalf("退避应从 30 秒翻倍到 1 小时封顶: %v", gaps)
	}
	if now.After(t0.Add(RetryWindow)) {
		t.Fatalf("重试不应超出 24 小时窗口: %s", now)
	}
	if attempts > MaxAttempts || attempts < 20 {
		t.Fatalf("24 小时内的次数应在上限内且不少于 20 次，得到 %d", attempts)
	}
	if _, ok := NextAttempt(t0, MaxAttempts, t0); ok {
		t.Fatal("达到次数上限后不再重试")
	}
}

func evt(tenant, typ string) *eventsv1.PlatformEvent {
	return &eventsv1.PlatformEvent{EventId: "evt_1", Type: typ, Tenant: tenant, ObjectId: "bat_1", OccurredAt: timestamppb.New(t0)}
}

func TestMatches(t *testing.T) {
	types := []string{outbox.BatchCompleted}
	if !Matches("credit", types, evt("credit", outbox.BatchCompleted)) {
		t.Error("同租户、订阅了的事件应匹配")
	}
	if Matches("risk", types, evt("credit", outbox.BatchCompleted)) {
		t.Error("别的租户的事件不应发给本租户")
	}
	if Matches("credit", types, evt("credit", outbox.BatchFailed)) {
		t.Error("没订阅的事件不应匹配")
	}
	if !Matches("risk", []string{outbox.IndexVersionLive}, evt("", outbox.IndexVersionLive)) {
		t.Error("不带租户的事件发给所有订阅了它的租户")
	}
}

func TestPayloadCarriesNoTenantOrContent(t *testing.T) {
	b, err := Payload(evt("credit", outbox.BatchCompleted))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if len(m) != 4 || m["event_id"] != "evt_1" || m["type"] != "batch.completed" || m["object_id"] != "bat_1" || m["occurred_at"] != "2026-10-06T09:00:00Z" {
		t.Fatalf("事件体只应有 ID、类型、时间与对象 ID: %s", b)
	}
}

func TestValidation(t *testing.T) {
	for raw, allowHTTP := range map[string]bool{"https://hooks.corp.example/x": false, "http://e2e:8099/hook": true} {
		if err := ValidURL(raw, allowHTTP); err != nil {
			t.Errorf("%s 应合法: %v", raw, err)
		}
	}
	for _, raw := range []string{"http://hooks.corp.example/x", "https://u:p@hooks.corp.example/", "ftp://x/", "not a url", "https:///nohost"} {
		if ValidURL(raw, false) == nil {
			t.Errorf("%s 应被拒绝", raw)
		}
	}
	if ValidTypes([]string{outbox.BatchCompleted, outbox.IndexVersionLive}) != nil {
		t.Error("已知事件应合法")
	}
	for _, types := range [][]string{nil, {"batch.started"}, {outbox.BatchFailed, outbox.BatchFailed}} {
		if ValidTypes(types) == nil {
			t.Errorf("%v 应被拒绝", types)
		}
	}
}

// ---- 投递 ----

type fakeDeliveries struct {
	mu        sync.Mutex
	due       []Delivery
	delivered []int
	failed    []string
	next      []time.Time
}

func (f *fakeDeliveries) Claim(context.Context, int, time.Duration) ([]Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.due
	f.due = nil
	return out, nil
}

func (f *fakeDeliveries) Delivered(_ context.Context, _ Delivery, code int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, code)
	return nil
}

func (f *fakeDeliveries) Failed(_ context.Context, _ Delivery, _ int, msg string, next time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed, f.next = append(f.failed, msg), append(f.next, next)
	return nil
}

type fakeVault struct {
	data map[string]map[string]string
	err  error
}

func (f fakeVault) ReadKV(_ context.Context, _, path string) (map[string]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.data[path], nil
}

type received struct {
	mu      sync.Mutex
	headers []http.Header
	bodies  [][]byte
}

func receiver(t *testing.T, code int) (*httptest.Server, *received) {
	r := &received{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.headers, r.bodies = append(r.headers, req.Header.Clone()), append(r.bodies, b)
		r.mu.Unlock()
		if code == http.StatusFound {
			http.Redirect(w, req, "https://elsewhere.example/", code)
			return
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv, r
}

func deliverer(st DeliveryStore, v SecretReader) *Deliverer {
	return &Deliverer{Store: st, Secrets: v, Mount: "secret", HTTP: NewHTTPClient(), Log: quietLog(),
		Now: func() time.Time { return t0 }, Concurrency: 4, PollInterval: time.Millisecond}
}

func delivery(url string, active bool) Delivery {
	body, _ := Payload(evt("credit", outbox.BatchCompleted))
	return Delivery{EventID: "evt_1", SubscriptionID: "whk_1", Payload: body, Attempts: 1, WindowStart: t0, URL: url, SecretPath: secretPath("whk_1"), Active: active}
}

var vault = fakeVault{data: map[string]map[string]string{secretPath("whk_1"): {"current": "whsec_new", "previous": "whsec_old"}}}

func TestDeliverSignsAndRecords(t *testing.T) {
	srv, got := receiver(t, http.StatusNoContent)
	st := &fakeDeliveries{due: []Delivery{delivery(srv.URL, true)}}
	if n, err := deliverer(st, vault).Once(context.Background()); n != 1 || err != nil {
		t.Fatalf("应投递 1 条: %d %v", n, err)
	}
	if len(st.delivered) != 1 || st.delivered[0] != 204 {
		t.Fatalf("2xx 应记为送达: %+v", st)
	}
	h := got.headers[0]
	ts := h.Get(HeaderTimestamp)
	if h.Get(HeaderID) != "evt_1" || ts != strconv.FormatInt(t0.Unix(), 10) || h.Get("Content-Type") != "application/json" {
		t.Fatalf("请求头不对: %v", h)
	}
	if !verify("whsec_new", h.Get(HeaderSignature), ts, got.bodies[0]) || !verify("whsec_old", h.Get(HeaderSignature), ts, got.bodies[0]) {
		t.Fatalf("接收方应能用新旧密钥校验签名: %s", h.Get(HeaderSignature))
	}
}

func TestDeliverFailuresScheduleRetries(t *testing.T) {
	for name, code := range map[string]int{"接收方出错": 500, "重定向不跟随": http.StatusFound} {
		t.Run(name, func(t *testing.T) {
			srv, got := receiver(t, code)
			st := &fakeDeliveries{due: []Delivery{delivery(srv.URL, true)}}
			if _, err := deliverer(st, vault).Once(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(st.delivered) != 0 || len(st.failed) != 1 || st.failed[0] != "HTTP "+strconv.Itoa(code) {
				t.Fatalf("应记失败: %+v", st)
			}
			if !st.next[0].Equal(t0.Add(30 * time.Second)) {
				t.Fatalf("第一次失败后 30 秒重试，得到 %s", st.next[0])
			}
			if len(got.headers) != 1 {
				t.Fatalf("重定向不应被跟随，接收方应只收到 1 次: %d", len(got.headers))
			}
		})
	}
}

func TestDeliverUnreachableAndSecretErrors(t *testing.T) {
	st := &fakeDeliveries{due: []Delivery{delivery("http://127.0.0.1:1/unreachable", true)}}
	if _, err := deliverer(st, vault).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.failed) != 1 || st.failed[0] != errSend.Error() || st.next[0].IsZero() {
		t.Fatalf("连不上应稍后重试: %+v", st)
	}
	srv, got := receiver(t, 200)
	st = &fakeDeliveries{due: []Delivery{delivery(srv.URL, true)}}
	if _, err := deliverer(st, fakeVault{err: errors.New("vault 不可用")}).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got.headers) != 0 || len(st.failed) != 1 || st.failed[0] != errSecret.Error() || st.next[0].IsZero() {
		t.Fatalf("取不到密钥时不发送、稍后重试: %+v", st)
	}
}

func TestDeliverToInactiveSubscriptionGivesUp(t *testing.T) {
	srv, got := receiver(t, 200)
	st := &fakeDeliveries{due: []Delivery{delivery(srv.URL, false)}}
	if _, err := deliverer(st, vault).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got.headers) != 0 || len(st.failed) != 1 || !st.next[0].IsZero() {
		t.Fatalf("停用的订阅不发送、不重试: %+v", st)
	}
}

func TestDeliverGivesUpAfterWindow(t *testing.T) {
	srv, _ := receiver(t, 503)
	d := delivery(srv.URL, true)
	d.WindowStart = t0.Add(-RetryWindow + time.Second)
	st := &fakeDeliveries{due: []Delivery{d}}
	if _, err := deliverer(st, vault).Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.failed) != 1 || !st.next[0].IsZero() {
		t.Fatalf("超出 24 小时窗口应放弃: %+v", st)
	}
}

// ---- 消费事件流 ----

type fakeEnqueuer struct {
	err  error
	seen []string
}

func (f *fakeEnqueuer) Enqueue(_ context.Context, e *eventsv1.PlatformEvent) (int64, error) {
	f.seen = append(f.seen, e.GetEventId())
	return 1, f.err
}

func TestProcess(t *testing.T) {
	_, pb, err := outbox.New(outbox.BatchCompleted, "credit", "bat_1", t0)
	if err != nil {
		t.Fatal(err)
	}
	q := &fakeEnqueuer{}
	c := &Consumer{Store: q, Log: quietLog()}
	if !c.Process(context.Background(), map[string]any{"pb": string(pb)}) || len(q.seen) != 1 {
		t.Fatal("登记成功后应确认")
	}
	if !c.Process(context.Background(), map[string]any{"pb": "\xff"}) {
		t.Fatal("无法解析的消息应确认（重读也解析不了）")
	}
	q.err = errors.New("数据库不可用")
	if c.Process(context.Background(), map[string]any{"pb": string(pb)}) {
		t.Fatal("登记失败时不应确认，之后重读")
	}
}

// ---- 订阅管理 ----

type fakeAdminStore struct {
	AdminStore
	created []Subscription
}

func (f *fakeAdminStore) CreateSubscription(_ context.Context, s Subscription) (Subscription, error) {
	s.Active, s.CreatedAt = true, t0
	f.created = append(f.created, s)
	return s, nil
}

type fakeSecretWriter struct {
	writes map[string]map[string]string
}

func (f *fakeSecretWriter) WriteKV(_ context.Context, _, path string, data map[string]string) error {
	f.writes[path] = data
	return nil
}

func TestCreateSubscription(t *testing.T) {
	st, sw := &fakeAdminStore{}, &fakeSecretWriter{writes: map[string]map[string]string{}}
	a := &Admin{Store: st, Secrets: sw, Mount: "secret", Log: quietLog()}
	resp, err := a.CreateSubscription(context.Background(), &adminv1.CreateSubscriptionRequest{
		Tenant: "credit", Url: "https://hooks.corp.example/bf", EventTypes: []string{outbox.BatchCompleted, outbox.BatchFailed},
	})
	if err != nil {
		t.Fatal(err)
	}
	sub := resp.GetSubscription()
	if !strings.HasPrefix(sub.GetSubscriptionId(), "whk_") || !sub.GetActive() || !strings.HasPrefix(resp.GetSigningSecret(), "whsec_") {
		t.Fatalf("订阅或密钥不对: %v", resp)
	}
	path := secretPath(sub.GetSubscriptionId())
	if sw.writes[path]["current"] != resp.GetSigningSecret() || st.created[0].SecretPath != path {
		t.Fatalf("密钥应存进 Vault，库里只记路径: %v %+v", sw.writes, st.created[0])
	}
	for name, r := range map[string]*adminv1.CreateSubscriptionRequest{
		"http 地址": {Tenant: "credit", Url: "http://hooks.corp.example/bf", EventTypes: []string{outbox.BatchCompleted}},
		"未知事件":    {Tenant: "credit", Url: "https://hooks.corp.example/bf", EventTypes: []string{"doc.deleted"}},
		"租户不合法":   {Tenant: "Credit", Url: "https://hooks.corp.example/bf", EventTypes: []string{outbox.BatchCompleted}},
	} {
		if _, err := a.CreateSubscription(context.Background(), r); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s 应返回 INVALID_ARGUMENT，得到 %v", name, err)
		}
	}
	if len(st.created) != 1 || len(sw.writes) != 1 {
		t.Fatal("被拒绝的登记不应写库或写密钥")
	}
}
