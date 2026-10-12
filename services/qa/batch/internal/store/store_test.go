package store

import (
	"testing"

	"github.com/hei-/bambuflow/libs/platform-go/outbox"
)

func TestFinalStatusAndEvent(t *testing.T) {
	cases := []struct {
		succeeded, cancelled int64
		status, event        string
	}{
		{3, 0, "completed", outbox.BatchCompleted},
		{1, 0, "completed", outbox.BatchCompleted},
		{0, 0, "failed", outbox.BatchFailed},
		// 被取消的批次以 cancelled 结束，不发事件：取消是提交人自己发起的
		{2, 1, "cancelled", ""},
		{0, 5, "cancelled", ""},
	}
	for _, c := range cases {
		got := FinalStatus(c.succeeded, c.cancelled)
		if got != c.status || EventType(got) != c.event {
			t.Errorf("成功 %d、取消 %d：得到 %s/%q，应为 %s/%q", c.succeeded, c.cancelled, got, EventType(got), c.status, c.event)
		}
	}
}
