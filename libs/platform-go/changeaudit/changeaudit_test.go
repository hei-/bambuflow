package changeaudit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type memStore struct{ keys, bodies []string }

func (m *memStore) Put(_ context.Context, key string, body []byte) error {
	m.keys, m.bodies = append(m.keys, key), append(m.bodies, string(body))
	return nil
}

func TestServiceActorWithoutPrincipal(t *testing.T) {
	st := &memStore{}
	w := &Writer{Store: st, Service: "governance", Now: func() time.Time { return time.Date(2026, 10, 7, 1, 0, 0, 0, time.UTC) }}
	if err := w.Write(AsService(context.Background(), "evaluation"), "experiment.stop", "exp_1", nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), "experiment.stop", "exp_2", nil); err != nil {
		t.Fatal(err)
	}
	var first, second Record
	_ = json.Unmarshal([]byte(st.bodies[0]), &first)
	_ = json.Unmarshal([]byte(st.bodies[1]), &second)
	// 没有 Principal 也没标记的是写审计的服务自己的定时任务
	if first.Actor != "service:evaluation" || second.Actor != "service:governance" {
		t.Fatalf("得到 %q、%q", first.Actor, second.Actor)
	}
	if !strings.HasPrefix(st.keys[0], Prefix("governance")+"2026/10/07/") {
		t.Fatalf("键 %s", st.keys[0])
	}
}
