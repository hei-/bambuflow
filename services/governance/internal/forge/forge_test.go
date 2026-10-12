package forge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const (
	repoFile = "config/tenants/credit/pipeline.yaml"
	newBody  = "answer:\n  support_threshold: 0.6\n"
)

func mr() MergeRequest {
	files := map[string]string{repoFile: newBody}
	return MergeRequest{ID: "chg_1", Title: "credit pipeline：调高支持度阈值", Description: "提交人 alice", Files: files}
}

func TestOutboxWritesFilesUnderChangeID(t *testing.T) {
	dir := t.TempDir()
	link, err := Outbox{Dir: dir}.Open(context.Background(), mr())
	if err != nil || link != "file://"+filepath.Join(dir, "chg_1") {
		t.Fatalf("%s %v", link, err)
	}
	b, err := os.ReadFile(filepath.Clean(filepath.Join(dir, "chg_1", "config", "tenants", "credit", "pipeline.yaml")))
	if err != nil || !strings.Contains(string(b), "0.6") {
		t.Fatalf("%s %v", b, err)
	}
	bad := mr()
	bad.Files = map[string]string{"../../etc/x": "x"}
	if _, err := (Outbox{Dir: dir}).Open(context.Background(), bad); err == nil {
		t.Fatal("越出目录的路径应拒绝")
	}
}

func TestGitLabCommitsThenOpensMergeRequest(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	var commit map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("PRIVATE-TOKEN") != "tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		calls = append(calls, r.Method+" "+r.URL.EscapedPath())
		switch {
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/repository/commits"):
			_ = json.NewDecoder(r.Body).Decode(&commit)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"abc"}`))
		case strings.HasSuffix(r.URL.Path, "/merge_requests"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"web_url":"https://git.corp/platform/config/-/merge_requests/7"}`))
		}
	}))
	defer srv.Close()
	g := &GitLab{BaseURL: srv.URL, Project: "platform/config", Token: "tok", Target: "main", Client: srv.Client()}
	link, err := g.Open(context.Background(), mr())
	if err != nil || link != "https://git.corp/platform/config/-/merge_requests/7" {
		t.Fatalf("%s %v", link, err)
	}
	want := []string{
		"HEAD /api/v4/projects/platform%2Fconfig/repository/files/config%2Ftenants%2Fcredit%2Fpipeline.yaml",
		"POST /api/v4/projects/platform%2Fconfig/repository/commits",
		"POST /api/v4/projects/platform%2Fconfig/merge_requests",
	}
	if strings.Join(calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%v", calls)
	}
	acts, _ := commit["actions"].([]any)
	first, _ := acts[0].(map[string]any)
	if commit["branch"] != "bambuflow/chg_1" || commit["start_branch"] != "main" || first["action"] != "create" {
		t.Fatalf("%v", commit)
	}
	g.Token = "wrong"
	if _, err := g.Open(context.Background(), mr()); err == nil {
		t.Fatal("令牌不对应返回错误")
	}
}

func TestFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	f, err := FromEnv(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Open(context.Background(), mr()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("不配置时应拒绝: %v", err)
	}
	if _, err := FromEnv(env(map[string]string{"BF_GIT_FORGE": "outbox"})); err == nil {
		t.Fatal("outbox 缺目录应报错")
	}
	if _, err := FromEnv(env(map[string]string{"BF_GIT_FORGE": "github"})); err == nil {
		t.Fatal("未知实现应报错")
	}
}
