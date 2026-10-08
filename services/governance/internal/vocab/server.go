package vocab

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// Snapshots 是 Server 读取词表的来源（生产为 Store）。
type Snapshots interface {
	Snapshot(ctx context.Context) (string, []Term, error)
}

// Server 实现 ConfigService。retrieval 拿到的是租户授权（Retrieval），其他服务拿到词表，orchestrator 另有运行中的实验；
// 签名、Watch、Ack 随其他配置对象接入时补上（governance 详设 7.1）。每个服务只拿自己需要的对象。
type Server struct {
	configv1.UnimplementedConfigServiceServer
	Store Snapshots
	// Retrieval 生成 retrieval 的快照（internal/access 的租户授权）
	Retrieval func(ctx context.Context) (*configv1.GetSnapshotResponse, error)
	// Experiments 是 orchestrator 快照里的运行中实验与它们的摘要（internal/experiment）
	Experiments func(ctx context.Context) ([]*configv1.RunningExperiment, string, error)
	Log         *slog.Logger
}

func (s *Server) GetSnapshot(ctx context.Context, r *configv1.GetSnapshotRequest) (*configv1.GetSnapshotResponse, error) {
	if r.GetService() == "" {
		return nil, status.Error(codes.InvalidArgument, "需要 service")
	}
	if r.GetService() == "retrieval" {
		resp, err := s.Retrieval(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取租户授权失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "租户授权暂不可用")
		}
		return resp, nil
	}
	version, terms, err := s.Store.Snapshot(ctx)
	if err != nil {
		s.Log.ErrorContext(ctx, "读取词表失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "词表暂不可用")
	}
	resp := &configv1.GetSnapshotResponse{SnapshotVersion: version}
	if r.GetService() == "orchestrator" {
		exps, digest, err := s.Experiments(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取运行中的实验失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "实验配置暂不可用")
		}
		// 词表与实验任一变化都换新的快照版本（回放凭它还原，governance 详设 3.3）
		resp.SnapshotVersion, resp.Experiments = version+"+exp:"+digest, exps
	}
	byName := map[string]*configv1.Vocabulary{}
	for _, n := range Names {
		v := &configv1.Vocabulary{Name: n}
		byName[n] = v
		resp.Vocabularies = append(resp.Vocabularies, v)
	}
	for _, t := range terms {
		st := configv1.TermStatus_TERM_STATUS_ACTIVE
		if t.Status == "deprecated" {
			st = configv1.TermStatus_TERM_STATUS_DEPRECATED
		}
		byName[t.Vocabulary].Terms = append(byName[t.Vocabulary].Terms, &configv1.VocabularyTerm{
			Code: t.Code, Name: t.Name, Synonyms: t.Synonyms, Parent: t.Parent, Status: st, ReplacedBy: t.ReplacedBy,
		})
	}
	s.Log.InfoContext(ctx, "下发词表快照", slog.String("service", r.GetService()), slog.String("snapshot_version", version))
	return resp, nil
}
