package outbox

import (
	"strings"
	"testing"
	"time"
)

func TestNewAndDecodeRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)
	evt, pb, err := New(BatchCompleted, "credit", "bat_1", at)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(evt.GetEventId(), "evt_") {
		t.Fatalf("event_id 应带 evt_ 前缀: %q", evt.GetEventId())
	}
	// 从事件流读回来的字段值是字符串
	got, err := Decode(map[string]any{"event_id": evt.GetEventId(), "pb": string(pb)})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetEventId() != evt.GetEventId() || got.GetType() != BatchCompleted || got.GetTenant() != "credit" ||
		got.GetObjectId() != "bat_1" || !got.GetOccurredAt().AsTime().Equal(at) {
		t.Fatalf("往返后不一致: %v", got)
	}
}

func TestNewGivesDistinctIDs(t *testing.T) {
	a, _, _ := New(IndexVersionLive, "", "ix-1", time.Now())
	b, _, _ := New(IndexVersionLive, "", "ix-1", time.Now())
	if a.GetEventId() == b.GetEventId() {
		t.Fatal("两条事件的 event_id 相同")
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for name, v := range map[string]map[string]any{
		"缺少 pb":   {"event_id": "evt_1"},
		"pb 不是串":  {"pb": 42},
		"pb 无法解析": {"pb": "\xff\xff\xff"},
		"缺少类型":    {"pb": ""},
	} {
		if _, err := Decode(v); err == nil {
			t.Errorf("%s: 应报错", name)
		}
	}
}
