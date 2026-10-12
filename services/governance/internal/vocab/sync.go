package vocab

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

// RolePlatformOperator 与平台管理员一起看快照的分发情况（governance 详设 9.1）。
const RolePlatformOperator = "platform_operator"

// syncServices 是取快照的服务；syncWindow 内上报过的实例才列出（processing 只在启动时上报，所以窗口取一天）。
var syncServices = []string{"gateway", "retrieval", "orchestrator", "processing"}

const syncWindow = 24 * time.Hour

// Ack 是一个实例最近一次上报的加载结果。
type Ack struct {
	Service, Instance, Version, Reason string
	Loaded                             bool
	AckedAt                            time.Time
}

// AckLister 列出 since 之后上报过的实例（生产为 Store）。
type AckLister interface {
	RecentAcks(ctx context.Context, since time.Time) ([]Ack, error)
}

// SyncServer 实现 SnapshotSyncAdminService：各服务当前应生效的快照版本，与每个实例加载到哪一版（governance 详设 8.2）。
type SyncServer struct {
	adminv1.UnimplementedSnapshotSyncAdminServiceServer
	// Config 生成各服务当前的快照（与下发的是同一份）
	Config *Server
	Acks   AckLister
	Now    func() time.Time
	Log    *slog.Logger
}

func (s *SyncServer) ListSnapshotSync(ctx context.Context, _ *adminv1.ListSnapshotSyncRequest) (*adminv1.ListSnapshotSyncResponse, error) {
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), RolePlatformAdmin, RolePlatformOperator) {
		return nil, status.Error(codes.PermissionDenied, "需要平台管理员或平台运维")
	}
	acks, err := s.Acks.RecentAcks(ctx, s.Now().Add(-syncWindow))
	if err != nil {
		s.Log.ErrorContext(ctx, "读取加载结果失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	byService := map[string][]Ack{}
	for _, a := range acks {
		byService[a.Service] = append(byService[a.Service], a)
	}
	out := &adminv1.ListSnapshotSyncResponse{}
	for _, svc := range syncServices {
		// 读不到当前版本（如元数据库不可用）时照样列出实例，只是都算没跟上
		current := ""
		if snap, err := s.Config.build(ctx, svc); err == nil {
			current = snap.GetSnapshotVersion()
		}
		ss := &adminv1.ServiceSync{Service: svc, CurrentVersion: current}
		for _, a := range byService[svc] {
			ss.Instances = append(ss.Instances, &adminv1.InstanceSync{InstanceId: a.Instance, SnapshotVersion: a.Version, Loaded: a.Loaded,
				Reason: a.Reason, AckedAt: timestamppb.New(a.AckedAt), InSync: current != "" && a.Loaded && a.Version == current})
		}
		out.Services = append(out.Services, ss)
	}
	return out, nil
}
