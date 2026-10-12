package vocab

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

type listedAcks struct {
	acks  []Ack
	since time.Time
}

func (l *listedAcks) RecentAcks(_ context.Context, since time.Time) ([]Ack, error) {
	l.since = since
	return l.acks, nil
}

func TestListSnapshotSyncMarksLaggingInstances(t *testing.T) {
	now := time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)
	terms := fixed{{Vocabulary: "topic", Code: "T-1", Name: "信贷", Status: "active"}}
	cfg := &Server{Store: terms, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Retrieval: func(context.Context) (*configv1.GetSnapshotResponse, error) {
			return nil, errors.New("元数据库不可用")
		},
		Tenants:     func(context.Context) ([]*configv1.TenantConfig, string, error) { return nil, "abc", nil },
		Experiments: func(context.Context, string) ([]*configv1.RunningExperiment, string, error) { return nil, "e1", nil },
		Pipelines:   func(context.Context) ([]*configv1.PipelineConfig, string, error) { return nil, "p1", nil },
		Routing:     func(context.Context) (*configv1.RoutingConfig, string, error) { return nil, "", nil },
		Prompts:     func(context.Context) ([]*configv1.PromptTemplate, string, error) { return nil, "q1", nil },
		Canaries:    func(context.Context, string) ([]*configv1.Canary, string, error) { return nil, "c1", nil }}
	vocabVersion := Version(terms)
	acks := &listedAcks{acks: []Ack{
		{Service: "gateway", Instance: "gw-1", Version: "tenants:abc", Loaded: true, AckedAt: now},
		{Service: "gateway", Instance: "gw-2", Version: "tenants:old", Loaded: true, AckedAt: now},
		{Service: "gateway", Instance: "gw-3", Version: "tenants:abc", Reason: "invalid_config", AckedAt: now},
		{Service: "processing", Instance: "p-1", Version: vocabVersion, Loaded: true, AckedAt: now.Add(-3 * time.Hour)},
		{Service: "retrieval", Instance: "r-1", Version: "grants:1", Loaded: true, AckedAt: now},
	}}
	s := &SyncServer{Config: cfg, Acks: acks, Now: func() time.Time { return now }, Log: cfg.Log}

	if _, err := s.ListSnapshotSync(as(t, "auditor"), &adminv1.ListSnapshotSyncRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能看: %v", err)
	}
	resp, err := s.ListSnapshotSync(as(t, RolePlatformOperator), &adminv1.ListSnapshotSyncRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !acks.since.Equal(now.Add(-24*time.Hour)) || len(resp.GetServices()) != 4 {
		t.Fatalf("%v %v", acks.since, resp)
	}
	gw := resp.GetServices()[0]
	if gw.GetCurrentVersion() != "tenants:abc" || !gw.GetInstances()[0].GetInSync() || gw.GetInstances()[1].GetInSync() ||
		gw.GetInstances()[2].GetInSync() {
		t.Fatalf("gateway: %v", gw)
	}
	// 读不到当前版本时实例照样列出、都算没跟上
	if r := resp.GetServices()[1]; r.GetCurrentVersion() != "" || len(r.GetInstances()) != 1 || r.GetInstances()[0].GetInSync() {
		t.Fatalf("retrieval: %v", r)
	}
	if o := resp.GetServices()[2]; o.GetCurrentVersion() != vocabVersion+"+exp:e1+pl:p1+rt:+pr:q1+ca:c1" || len(o.GetInstances()) != 0 {
		t.Fatalf("orchestrator: %v", o)
	}
	if p := resp.GetServices()[3]; !p.GetInstances()[0].GetInSync() {
		t.Fatalf("processing: %v", p)
	}
}
