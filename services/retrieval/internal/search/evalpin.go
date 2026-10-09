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

var versionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// errPinRefused：线上实例收到了指定索引版本的请求。
var errPinRefused = status.Error(codes.PermissionDenied, "只有评测模式的实例接受指定索引版本")

// EvalPinInterceptor 在进入任何处理函数之前检查 EvalIndexHeader：非评测模式的实例收到就拒绝。
func EvalPinInterceptor(evalMode bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := pinnedVersion(ctx); ok && !evalMode {
			return nil, errPinRefused
		}
		return handler(ctx, req)
	}
}

func pinnedVersion(ctx context.Context) (string, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get(EvalIndexHeader)
	if len(v) == 0 || v[0] == "" {
		return "", false
	}
	return v[0], true
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
