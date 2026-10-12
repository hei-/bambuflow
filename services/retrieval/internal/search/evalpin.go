package search

import (
	"context"
	"errors"
	"regexp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// EvalIndexHeader 是评测指定索引版本的 metadata（evaluation 详设 4.2）：门禁要在待发布版本上线之前评测它。
// 只有评测模式的实例（部署时开启 BF_EVAL_MODE，不接任何来自 gateway 的流量）接受；线上实例收到一律拒绝，
// 否则它就是一个绕过索引发布门禁的后门。
const EvalIndexHeader = "x-eval-index-version"

// EvalConfigHeader 是评测指定候选配置版本的 metadata（governance 详设 4.1 第 ③④ 步）：配置发布的门禁要在候选生效之前评测它。
// 规则与 EvalIndexHeader 相同：只有评测模式的实例接受。
const EvalConfigHeader = "x-eval-config-version"

var versionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// errPinRefused：线上实例收到了指定索引版本或候选配置的请求。
var errPinRefused = status.Error(codes.PermissionDenied, "只有评测模式的实例接受指定索引版本或候选配置")

// Candidates 在评测模式下按 EvalConfigHeader 取候选配置，放进 ctx 供这次请求用（grants.Watcher）。
type Candidates interface {
	WithCandidate(ctx context.Context, version string) (context.Context, error)
}

// EvalPinInterceptor 在进入任何处理函数之前检查 EvalIndexHeader 与 EvalConfigHeader：非评测模式的实例收到就拒绝；
// 评测模式下按 EvalConfigHeader 取候选配置。要排在校验 Principal 的拦截器之后：取候选时转发这次请求的评测身份。
func EvalPinInterceptor(evalMode bool, cands Candidates) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_, pinned := pinnedVersion(ctx)
		cand := header(ctx, EvalConfigHeader)
		if !evalMode && (pinned || cand != "") {
			return nil, errPinRefused
		}
		if cand != "" {
			var err error
			if ctx, err = cands.WithCandidate(ctx, cand); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func header(ctx context.Context, key string) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func pinnedVersion(ctx context.Context) (string, bool) {
	v := header(ctx, EvalIndexHeader)
	return v, v != ""
}

// indexVersion 是本次请求要读的索引版本与它的嵌入模型：评测模式下按 EvalIndexHeader 指定的版本，否则是 prod 别名。
func (s *Server) indexVersion(ctx context.Context) (string, string, error) {
	v, ok := pinnedVersion(ctx)
	if !ok {
		return s.Store.LiveVersion(ctx)
	}
	// 拦截器之外再判断一次：没有经过拦截器的调用（如测试或以后新加的入口）也不能放过
	if !s.EvalMode {
		return "", "", errPinRefused
	}
	if !versionPattern.MatchString(v) {
		return "", "", status.Error(codes.InvalidArgument, "索引版本不合法")
	}
	model, err := s.Store.VersionModel(ctx, v)
	if errors.Is(err, ErrNoLiveIndex) {
		return "", "", status.Error(codes.NotFound, "索引版本不存在或已丢弃")
	}
	return v, model, err
}
