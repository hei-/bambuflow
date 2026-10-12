package vocab

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
)

// Snapshots 是 Server 读取词表的来源（生产为 Store）。
type Snapshots interface {
	Snapshot(ctx context.Context) (string, []Term, error)
}

// AckStore 记下各实例上报的加载结果（生产为 Store）。
type AckStore interface {
	SaveAck(ctx context.Context, a *configv1.AckRequest) error
}

// Server 实现 ConfigService（governance 详设 7.1）。retrieval 拿到的是租户授权（Retrieval），gateway 拿到生效的租户配置
// （Tenants），其他服务拿到词表，orchestrator 另有运行中的实验；每个服务只拿自己需要的对象。快照签名后下发；Watch 只推版本变化的通知。
type Server struct {
	configv1.UnimplementedConfigServiceServer
	Store Snapshots
	// Retrieval 生成 retrieval 的快照（internal/access 的租户授权）
	Retrieval func(ctx context.Context) (*configv1.GetSnapshotResponse, error)
	// Experiments 是某个服务快照里的运行中实验与它们的摘要（internal/experiment）
	Experiments func(ctx context.Context, service string) ([]*configv1.RunningExperiment, string, error)
	// Tenants 是 gateway 快照里生效的租户配置与它们的摘要（internal/config）
	Tenants func(ctx context.Context) ([]*configv1.TenantConfig, string, error)
	// PushSources 是 gateway 快照里接受推送入库的数据源与它们的摘要（internal/access）；为 nil 时不下发
	PushSources func(ctx context.Context) ([]*configv1.PushSource, string, error)
	// Pipelines 是 retrieval 与 orchestrator 快照里各租户生效的 pipeline，Routing 是 orchestrator 快照里的全局 routing
	// （internal/config）；摘要并入 snapshot_version
	Pipelines func(ctx context.Context) ([]*configv1.PipelineConfig, string, error)
	Routing   func(ctx context.Context) (*configv1.RoutingConfig, string, error)
	// Policy 是 retrieval 快照里合成好的检索权限策略包（internal/config）；没有生效的全局基线时为 nil
	Policy func(ctx context.Context) (*configv1.PolicyBundle, error)
	// Prompts 是 orchestrator 快照里经发布的回答模板（internal/config）
	Prompts func(ctx context.Context) ([]*configv1.PromptTemplate, string, error)
	// Canaries 是灰度中的候选版本（internal/config），按服务给出各自需要的对象
	Canaries func(ctx context.Context, service string) ([]*configv1.Canary, string, error)
	// Candidate 是评测副本按版本取的候选（internal/config）。调用方须是评测身份：GetCandidate 由 grpcmw 拦截器按
	// evaluation 的签发方校验（cmd/governance），这里只认已校验的 Principal
	Candidate func(ctx context.Context, service, versionID string) (*configv1.Canary, error)
	Signer    *Signer
	Acks      AckStore
	// WatchEvery 是 Watch 检查版本变化的间隔
	WatchEvery time.Duration
	Log        *slog.Logger
}

func (s *Server) GetSnapshot(ctx context.Context, r *configv1.GetSnapshotRequest) (*configv1.GetSnapshotResponse, error) {
	if r.GetService() == "" {
		return nil, status.Error(codes.InvalidArgument, "需要 service")
	}
	resp, err := s.build(ctx, r.GetService())
	if err != nil {
		return nil, err
	}
	if err := s.sign(ctx, resp); err != nil {
		return nil, err
	}
	s.Log.InfoContext(ctx, "下发快照", slog.String("service", r.GetService()), slog.String("snapshot_version", resp.GetSnapshotVersion()))
	return resp, nil
}

// GetCandidate 给评测副本按版本下发一个候选（governance 详设 4.1 第 ③④ 步）：快照里只有这一项灰度，与 GetSnapshot 同样签名。
func (s *Server) GetCandidate(ctx context.Context, r *configv1.GetCandidateRequest) (*configv1.GetCandidateResponse, error) {
	p := grpcmw.PrincipalFrom(ctx)
	if p == nil {
		// 拦截器没配上时也不放行：候选只对评测身份开放
		return nil, status.Error(codes.Unauthenticated, "需要评测身份")
	}
	c, err := s.Candidate(ctx, r.GetService(), r.GetVersionId())
	if err != nil {
		return nil, err
	}
	resp := &configv1.GetSnapshotResponse{SnapshotVersion: "candidate:" + r.GetVersionId(), Canaries: []*configv1.Canary{c}}
	if err := s.sign(ctx, resp); err != nil {
		return nil, err
	}
	s.Log.InfoContext(ctx, "下发候选配置", slog.String("service", r.GetService()), slog.String("version_id", r.GetVersionId()),
		slog.String("object", c.GetObject()), slog.String("scope", c.GetScope()))
	return &configv1.GetCandidateResponse{Snapshot: resp}, nil
}

// sign 把快照本体（不含签名字段）序列化后签名；使用方验签后从 signed_payload 解析，不读外层字段。
func (s *Server) sign(ctx context.Context, resp *configv1.GetSnapshotResponse) error {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return status.Error(codes.Internal, "快照序列化失败")
	}
	if err := s.Signer.Sign(resp, payload); err != nil {
		s.Log.ErrorContext(ctx, "快照签名失败", slog.Any("err", err))
		return status.Error(codes.Unavailable, "快照暂不可用")
	}
	return nil
}

// build 生成某个服务的快照（未签名）。
func (s *Server) build(ctx context.Context, service string) (*configv1.GetSnapshotResponse, error) {
	if service == "retrieval" {
		resp, err := s.Retrieval(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取租户授权失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "租户授权暂不可用")
		}
		pipelines, digest, err := s.Pipelines(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取 pipeline 失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "pipeline 暂不可用")
		}
		bundle, err := s.Policy(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "合成策略包失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "策略包暂不可用")
		}
		canaries, caDigest, err := s.Canaries(ctx, service)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取灰度失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "灰度暂不可用")
		}
		exps, expDigest, err := s.Experiments(ctx, service)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取运行中的实验失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "实验配置暂不可用")
		}
		resp.Pipelines, resp.Policy, resp.Canaries, resp.Experiments = pipelines, bundle, canaries, exps
		resp.SnapshotVersion = resp.GetSnapshotVersion() + "+pl:" + digest + "+pol:" + bundle.GetVersion() + "+ca:" + caDigest +
			"+exp:" + expDigest
		return resp, nil
	}
	if service == "gateway" {
		tenants, digest, err := s.Tenants(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取租户配置失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "租户配置暂不可用")
		}
		resp := &configv1.GetSnapshotResponse{SnapshotVersion: "tenants:" + digest, Tenants: tenants}
		if s.PushSources != nil {
			push, pushDigest, err := s.PushSources(ctx)
			if err != nil {
				s.Log.ErrorContext(ctx, "读取推送数据源失败", slog.Any("err", err))
				return nil, status.Error(codes.Unavailable, "推送数据源暂不可用")
			}
			resp.PushSources, resp.SnapshotVersion = push, resp.GetSnapshotVersion()+"+push:"+pushDigest
		}
		return resp, nil
	}
	version, terms, err := s.Store.Snapshot(ctx)
	if err != nil {
		s.Log.ErrorContext(ctx, "读取词表失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "词表暂不可用")
	}
	resp := &configv1.GetSnapshotResponse{SnapshotVersion: version}
	if service == "orchestrator" {
		exps, digest, err := s.Experiments(ctx, service)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取运行中的实验失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "实验配置暂不可用")
		}
		pipelines, plDigest, err := s.Pipelines(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取 pipeline 失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "pipeline 暂不可用")
		}
		routing, rtDigest, err := s.Routing(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取 routing 失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "routing 暂不可用")
		}
		prompts, prDigest, err := s.Prompts(ctx)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取回答模板失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "回答模板暂不可用")
		}
		canaries, caDigest, err := s.Canaries(ctx, service)
		if err != nil {
			s.Log.ErrorContext(ctx, "读取灰度失败", slog.Any("err", err))
			return nil, status.Error(codes.Unavailable, "灰度暂不可用")
		}
		// 词表、实验、pipeline、routing、回答模板、灰度任一变化都换新的快照版本（回放凭它还原，governance 详设 3.3）
		resp.SnapshotVersion = version + "+exp:" + digest + "+pl:" + plDigest + "+rt:" + rtDigest + "+pr:" + prDigest + "+ca:" + caDigest
		resp.Experiments, resp.Pipelines, resp.Routing, resp.Prompts = exps, pipelines, routing, prompts
		resp.Canaries = canaries
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
	return resp, nil
}

// Watch 每隔 WatchEvery 检查一次该服务的快照版本，变了（或与使用方已加载的不同）就推一条通知。
// 读快照暂时失败时不断开，下一轮再试；使用方断线后自己重连。
func (s *Server) Watch(r *configv1.WatchRequest, stream grpc.ServerStreamingServer[configv1.WatchResponse]) error {
	if r.GetService() == "" {
		return status.Error(codes.InvalidArgument, "需要 service")
	}
	ctx := stream.Context()
	sent := r.GetLoaded()
	t := time.NewTicker(s.WatchEvery)
	defer t.Stop()
	for {
		if resp, err := s.build(ctx, r.GetService()); err == nil && resp.GetSnapshotVersion() != sent {
			if err := stream.Send(&configv1.WatchResponse{Service: r.GetService(), SnapshotVersion: resp.GetSnapshotVersion()}); err != nil {
				return err
			}
			sent = resp.GetSnapshotVersion()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (s *Server) Ack(ctx context.Context, r *configv1.AckRequest) (*configv1.AckResponse, error) {
	if r.GetService() == "" || r.GetInstanceId() == "" || len(r.GetInstanceId()) > 256 || len(r.GetReason()) > 512 {
		return nil, status.Error(codes.InvalidArgument, "需要 service 与 instance_id")
	}
	if err := s.Acks.SaveAck(ctx, r); err != nil {
		s.Log.ErrorContext(ctx, "记录加载结果失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	if !r.GetLoaded() {
		// 加载失败要有人看：实例在用旧快照（governance 详设 8.2）
		s.Log.WarnContext(ctx, "实例加载快照失败", slog.String("service", r.GetService()), slog.String("instance", r.GetInstanceId()),
			slog.String("snapshot_version", r.GetSnapshotVersion()), slog.String("reason", r.GetReason()))
	}
	return &configv1.AckResponse{}, nil
}

func (s *Server) GetSigningKeys(context.Context, *configv1.GetSigningKeysRequest) (*configv1.GetSigningKeysResponse, error) {
	return &configv1.GetSigningKeysResponse{Keys: s.Signer.Keys()}, nil
}
