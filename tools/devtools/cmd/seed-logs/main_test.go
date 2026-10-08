package main

import (
	"testing"
	"time"
)

func TestMessagesAreOrderedAndDeterministic(t *testing.T) {
	day := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	a, b := messages(day), messages(day)
	if len(a) != 36 || a[0] != b[0] || a[len(a)-1] != b[len(b)-1] {
		t.Fatalf("同一天应得到同一批消息: %d", len(a))
	}
	for i := 1; i < len(a); i++ {
		if !a[i].at.After(a[i-1].at) {
			t.Fatalf("消息时间应严格递增（Redis Streams 的 ID 由它决定）: %v %v", a[i-1].at, a[i].at)
		}
	}
	if last := a[len(a)-1].at; last.Sub(day) >= 8*time.Hour+15*time.Minute {
		t.Fatalf("应落在 08:00–08:15 的三个窗口里: %v", last)
	}
}
