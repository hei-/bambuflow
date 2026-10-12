package api

import (
	"context"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 配置快照的分发情况（governance 详设 7.1 Ack、8.2）：GET /v1/admin/snapshot-sync，由 governance 实现。

type instanceSyncJSON struct {
	InstanceID      string `json:"instance_id"`
	SnapshotVersion string `json:"snapshot_version"`
	Loaded          bool   `json:"loaded"`
	Reason          string `json:"reason,omitempty"`
	AckedAt         string `json:"acked_at"`
	InSync          bool   `json:"in_sync"`
}

type serviceSyncJSON struct {
	Service        string             `json:"service"`
	CurrentVersion string             `json:"current_version"`
	Instances      []instanceSyncJSON `json:"instances"`
}

type snapshotSyncJSON struct {
	Services []serviceSyncJSON `json:"services"`
}

func (s *Server) listSnapshotSync(ctx context.Context, _ *adminInput) (adminOutput, error) {
	r, err := s.SnapshotSync.ListSnapshotSync(ctx, &adminv1.ListSnapshotSyncRequest{})
	if err != nil {
		return adminOutput{}, err
	}
	out := snapshotSyncJSON{Services: []serviceSyncJSON{}}
	for _, svc := range r.GetServices() {
		j := serviceSyncJSON{Service: svc.GetService(), CurrentVersion: svc.GetCurrentVersion(), Instances: []instanceSyncJSON{}}
		for _, in := range svc.GetInstances() {
			j.Instances = append(j.Instances, instanceSyncJSON{InstanceID: in.GetInstanceId(), SnapshotVersion: in.GetSnapshotVersion(),
				Loaded: in.GetLoaded(), Reason: in.GetReason(), AckedAt: rfc3339(in.GetAckedAt()), InSync: in.GetInSync()})
		}
		out.Services = append(out.Services, j)
	}
	return adminOutput{body: out}, nil
}
