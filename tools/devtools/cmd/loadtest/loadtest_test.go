package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRateRampsThenHolds(t *testing.T) {
	c := &config{rate: 10, to: 100, ramp: 5 * time.Minute}
	for at, want := range map[time.Duration]float64{0: 10, 150 * time.Second: 55, 5 * time.Minute: 100, time.Hour: 100} {
		if got := c.rateAt(at); got != want {
			t.Fatalf("%s 时速率 %v，期望 %v", at, got, want)
		}
	}
	if (&config{rate: 50}).rateAt(time.Hour) != 50 {
		t.Fatal("不爬升时速率不变")
	}
}

func TestMixerSpreadsMultihopEvenly(t *testing.T) {
	m := newMixer(map[string][]string{kindSingle: {"s1", "s2"}, kindMulti: {"m1"}}, 0.2)
	multi := 0
	for range 100 {
		if k, _ := m.next(); k == kindMulti {
			multi++
		}
	}
	if multi != 20 {
		t.Fatalf("100 道里多跳题 %d 道，期望 20", multi)
	}
	// 没有多跳题时全部出单跳题
	m = newMixer(map[string][]string{kindSingle: {"s1"}}, 0.5)
	if k, q := m.next(); k != kindSingle || q != "s1" {
		t.Fatalf("没有多跳题: %s %s", k, q)
	}
}

func TestReadSSE(t *testing.T) {
	stream := ": 心跳\n\nevent: meta\ndata: {\"answer_id\":\"ans_1\"}\n\nevent: sentence\ndata: {\"text\":\"一\"}\n\n" +
		"event: final\ndata: {\"status\":\"answered\",\"why\":{\"degraded\":[{\"stage\":\"dense\",\"reason\":\"timeout\"}]}}\n\n"
	var events []string
	if err := readSSE(strings.NewReader(stream), func(e string, _ []byte) bool { events = append(events, e); return e != "final" }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(events, ",") != "meta,sentence,final" {
		t.Fatalf("事件: %v", events)
	}
	// 没收到 final 就断开
	if err := readSSE(strings.NewReader("event: sentence\ndata: {}\n\n"), func(string, []byte) bool { return true }); err == nil {
		t.Fatal("没有 final 时应报错")
	}
}

func TestRunAgainstFakeGateway(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body struct {
			Question string `json:"question"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch body.Question {
		case "限流":
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, `{"type":"rate_limited"}`)
		case "坏了":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "event: sentence\ndata: {}\n\nevent: final\ndata: {\"status\":\"answered\",\"why\":{}}\n\n")
		}
	}))
	defer srv.Close()
	c := &config{base: srv.URL, tokens: []string{"t1", "t2"}, rate: 100, duration: 500 * time.Millisecond, stream: true,
		maxInflight: 100, client: srv.Client(),
		questions: map[string][]string{kindSingle: {"正常", "正常", "限流", "坏了"}},
		criteria:  criteria{p95: time.Second, multihopP95: time.Second, errorRate: 0.001}}
	rep, err := run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	s := rep.Overall[kindSingle]
	if s.Requests < 20 || s.Limited == 0 || s.Errors == 0 || s.Statuses["answered"] == 0 {
		t.Fatalf("统计: %+v", s)
	}
	// 错误率 25% 超标：不通过；429 不算错误
	if rep.Passed || !strings.Contains(rep.markdown(), "不通过") {
		t.Fatalf("应判不通过: %+v", rep.Checks)
	}
	if float64(s.Limited)/float64(s.Requests) < 0.2 || float64(s.Errors)/float64(s.Requests) > 0.3 {
		t.Fatalf("429 与错误分开计: %+v", s)
	}
}
