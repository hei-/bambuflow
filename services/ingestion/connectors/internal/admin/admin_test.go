package admin

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/holds"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/syncstatus"
)

var quiet = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

type staticVerifier struct{ p *identityv1.Principal }

func (v staticVerifier) Verify(context.Context, string) (*identityv1.Principal, error) {
	return v.p, nil
}

func as(t *testing.T, sub string, roles ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer x"))
	var out context.Context
	if _, err := grpcmw.UnaryServer(staticVerifier{&identityv1.Principal{Sub: sub, Roles: roles}}, quiet)(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/x/y"}, func(ctx context.Context, _ any) (any, error) { out = ctx; return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSyncStatusPauseResume(t *testing.T) {
	mr := miniredis.RunT(t)
	st := syncstatus.Store{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	now := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)
	s := &Server{Store: st, Log: quiet, Now: func() time.Time { return now }}
	ops, owner, auditor := as(t, "olga", roleOperator), as(t, "dana", roleDataOwner), as(t, "audra", "auditor")

	if _, err := s.GetSyncStatus(ops, &adminv1.GetSyncStatusRequest{SourceId: "src_wiki"}); status.Code(err) != codes.NotFound {
		t.Fatalf("没有记录应 NotFound: %v", err)
	}
	run := syncstatus.Run{Command: "sync-wiki", StartedAt: now.Add(-time.Hour), Upserts: 2}
	run.Finish(nil, now.Add(-30*time.Minute))
	if err := st.Record(context.Background(), "src_wiki", run); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSyncStatus(owner, &adminv1.GetSyncStatusRequest{SourceId: "src_wiki"})
	if err != nil || got.GetFreshnessSeconds() != 1800 || got.GetLastSuccess().GetUpserts() != 2 || got.GetPause() != nil {
		t.Fatalf("数据属主看状态: %v %v", got, err)
	}
	if _, err := s.GetSyncStatus(auditor, &adminv1.GetSyncStatusRequest{SourceId: "src_wiki"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("审计岗不能看: %v", err)
	}
	if _, err := s.PauseSource(owner, &adminv1.PauseSourceRequest{SourceId: "src_wiki", Reason: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("数据属主不能暂停: %v", err)
	}
	if _, err := s.PauseSource(ops, &adminv1.PauseSourceRequest{SourceId: "src_wiki"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("暂停要写原因: %v", err)
	}
	if _, err := s.PauseSource(ops, &adminv1.PauseSourceRequest{SourceId: "wiki", Reason: "x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("source_id 要带 src_: %v", err)
	}
	p, err := s.PauseSource(ops, &adminv1.PauseSourceRequest{SourceId: "src_wiki", Reason: "源系统维护"})
	if err != nil || !p.GetChanged() || p.GetPause().GetPausedBy() != "olga" {
		t.Fatalf("暂停: %v %v", p, err)
	}
	got, _ = s.GetSyncStatus(ops, &adminv1.GetSyncStatusRequest{SourceId: "src_wiki"})
	if got.GetPause().GetReason() != "源系统维护" {
		t.Fatalf("状态里应有暂停: %v", got)
	}
	r, err := s.ResumeSource(ops, &adminv1.ResumeSourceRequest{SourceId: "src_wiki"})
	if err != nil || !r.GetChanged() {
		t.Fatalf("恢复: %v %v", r, err)
	}
	if r, _ := s.ResumeSource(ops, &adminv1.ResumeSourceRequest{SourceId: "src_wiki"}); r.GetChanged() {
		t.Fatal("已恢复再恢复应 changed=false")
	}
}

func TestPendingDeletionsNeedTwoOwners(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	s := &Server{Holds: holds.Store{Client: rdb}, Log: quiet, Now: func() time.Time { return now }}
	h, err := s.Holds.Observe(context.Background(), "src_wiki", []string{"d1", "d2"}, 30, now)
	if err != nil {
		t.Fatal(err)
	}
	dana, rick, olga := as(t, "dana", roleDataOwner), as(t, "rick", roleDataOwner), as(t, "olga", roleOperator)

	list, err := s.ListPendingDeletions(olga, &adminv1.ListPendingDeletionsRequest{SourceId: "src_wiki"})
	if err != nil || len(list.GetHolds()) != 1 || list.GetHolds()[0].GetStatus() != holds.Pending {
		t.Fatalf("平台运维能看: %v %v", list, err)
	}
	if _, err := s.ConfirmDeletion(olga, &adminv1.ConfirmDeletionRequest{SourceId: "src_wiki", HoldId: h.ID, Reason: "x"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("平台运维不能确认: %v", err)
	}
	if _, err := s.ConfirmDeletion(dana, &adminv1.ConfirmDeletionRequest{SourceId: "src_wiki", HoldId: h.ID}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("要写原因: %v", err)
	}
	first, err := s.ConfirmDeletion(dana, &adminv1.ConfirmDeletionRequest{SourceId: "src_wiki", HoldId: h.ID, Reason: "源端下线"})
	if err != nil || first.GetHold().GetStatus() != holds.AwaitingSecond {
		t.Fatalf("第一位确认: %v %v", first, err)
	}
	_, err = s.ConfirmDeletion(dana, &adminv1.ConfirmDeletionRequest{SourceId: "src_wiki", HoldId: h.ID, Reason: "再确认"})
	if status.Code(err) != codes.PermissionDenied || grpcmw.ReasonOf(err) != "self_approval_forbidden" {
		t.Fatalf("同一人不能复核: %v", err)
	}
	second, err := s.ConfirmDeletion(rick, &adminv1.ConfirmDeletionRequest{SourceId: "src_wiki", HoldId: h.ID, Reason: "复核"})
	if err != nil || second.GetHold().GetStatus() != holds.Confirmed || second.GetHold().GetApprovedBy() != "rick" {
		t.Fatalf("第二位确认: %v %v", second, err)
	}
	if _, err := s.DiscardDeletion(dana, &adminv1.DiscardDeletionRequest{SourceId: "src_wiki", HoldId: "hold_none", Reason: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("不存在的批次: %v", err)
	}
	d, err := s.DiscardDeletion(dana, &adminv1.DiscardDeletionRequest{SourceId: "src_wiki", HoldId: h.ID, Reason: "改主意"})
	if err != nil || d.GetHold().GetStatus() != holds.Discarded {
		t.Fatalf("执行前可以丢弃: %v %v", d, err)
	}
}
