package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fakeAPI(t *testing.T, crons, jobs string, created *map[string]any) *Kubernetes {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cronjobs"):
			if r.URL.Query().Get("labelSelector") != SourceLabel+"=src_wiki" {
				t.Errorf("selector %q", r.URL.Query().Get("labelSelector"))
			}
			_, _ = io.WriteString(w, crons)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/jobs"):
			_, _ = io.WriteString(w, jobs)
		case r.Method == http.MethodPost && r.URL.Path == "/apis/batch/v1/namespaces/ingest/jobs":
			_ = json.NewDecoder(r.Body).Decode(created)
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, "{}")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return &Kubernetes{Base: srv.URL, Namespace: "ingest", Token: func() (string, error) { return "tok", nil }, HTTP: srv.Client()}
}

const cron = `{"items":[{"metadata":{"name":"connectors-sync-wiki"},"spec":{"jobTemplate":{"metadata":{"labels":{"app":"connectors"}},
	"spec":{"template":{"spec":{"containers":[{"name":"sync","image":"connectors"}]}}}}}}]}`

func TestSyncCreatesJobFromCronJob(t *testing.T) {
	var created map[string]any
	k := fakeAPI(t, cron, `{"items":[{"status":{"active":0}}]}`, &created)
	name, err := k.Sync(context.Background(), "src_wiki")
	if err != nil || !strings.HasPrefix(name, "connectors-sync-wiki-manual-") {
		t.Fatalf("%q %v", name, err)
	}
	meta := created["metadata"].(map[string]any)
	labels := meta["labels"].(map[string]any)
	if meta["name"] != name || labels[SourceLabel] != "src_wiki" || labels["app"] != "connectors" {
		t.Fatalf("%v", meta)
	}
	if !strings.Contains(mustJSON(created["spec"]), `"image":"connectors"`) {
		t.Fatalf("沿用 CronJob 的任务模板: %v", created["spec"])
	}
}

func TestSyncRefusals(t *testing.T) {
	var created map[string]any
	if _, err := fakeAPI(t, `{"items":[]}`, `{"items":[]}`, &created).Sync(context.Background(), "src_wiki"); !errors.Is(err, ErrNoSchedule) {
		t.Fatal(err)
	}
	if _, err := fakeAPI(t, cron, `{"items":[{"status":{"active":1}}]}`, &created).Sync(context.Background(), "src_wiki"); !errors.Is(err, ErrRunning) {
		t.Fatal(err)
	}
	if created != nil {
		t.Fatal("拒绝时不创建任务")
	}
	if _, err := (Unconfigured{}).Sync(context.Background(), "src_wiki"); !errors.Is(err, ErrUnconfigured) {
		t.Fatal(err)
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
