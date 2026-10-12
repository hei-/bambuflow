// Package grpcmw 是服务间 gRPC 的公共拦截器（技术设计文档 12.5）：
// 身份只来自 metadata 中的 Principal；deadline 由 gateway 设置并逐跳下传，只能缩短。
package grpcmw

import (
	"context"
	"log/slog"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
)

const authorizationKey = "authorization"

type ctxKey struct{}

type caller struct {
	p   *identityv1.Principal
	raw string
}

// PrincipalFrom 返回经过校验的调用者身份；没有时返回 nil。
func PrincipalFrom(ctx context.Context) *identityv1.Principal {
	c, _ := ctx.Value(ctxKey{}).(caller)
	return c.p
}

// Verifier 校验原始 Principal 令牌。
type Verifier interface {
	Verify(ctx context.Context, raw string) (*identityv1.Principal, error)
}

// UnaryServer 返回服务端拦截器：先要求有 deadline，再校验 Principal。
// 校验失败一律返回不带细节的 UNAUTHENTICATED，原因只写日志，不让调用方据此试探。
func UnaryServer(v Verifier, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); !ok {
			return nil, status.Error(codes.InvalidArgument, "请求必须带 deadline")
		}
		raw := bearer(ctx)
		if raw == "" {
			return nil, status.Error(codes.Unauthenticated, "缺少 Principal")
		}
		p, err := v.Verify(ctx, raw)
		if err != nil {
			log.WarnContext(ctx, "Principal 校验失败", slog.String("method", info.FullMethod), slog.Any("err", err))
			return nil, status.Error(codes.Unauthenticated, "Principal 无效")
		}
		return handler(context.WithValue(ctx, ctxKey{}, caller{p: p, raw: raw}), req)
	}
}

func bearer(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get(authorizationKey)
	if len(vals) != 1 {
		return ""
	}
	raw, ok := strings.CutPrefix(vals[0], "Bearer ")
	if !ok {
		return ""
	}
	return raw
}

// ForwardPrincipal 把当前请求的 Principal 原样带到下游调用的 metadata 里。
// 下游按同一个 Principal 鉴权，服务不能替用户换身份；没有 Principal 时不附加，下游会拒绝。
func ForwardPrincipal(ctx context.Context) context.Context {
	c, ok := ctx.Value(ctxKey{}).(caller)
	if !ok {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, authorizationKey, "Bearer "+c.raw)
}

// UnaryServerFor 只对 services 列出的 gRPC 服务（全名，如 bambuflow.admin.v1.SourceAdminService）或单个方法
// （服务全名/方法名，如 bambuflow.config.v1.ConfigService/GetCandidate）做 UnaryServer 的检查，
// 同一进程里的其他服务原样放行。管理接口与服务间接口（如 ConfigService）共用一个端口时用它。
func UnaryServerFor(services []string, v Verifier, log *slog.Logger) grpc.UnaryServerInterceptor {
	check := UnaryServer(v, log)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		for _, s := range services {
			if info.FullMethod == "/"+s || strings.HasPrefix(info.FullMethod, "/"+s+"/") {
				return check(ctx, req, info, handler)
			}
		}
		return handler(ctx, req)
	}
}

// TraceIDKey 是 gateway 转发管理请求时附带的入口 trace_id：实现方的业务审计记下它，与 gateway 的管理审计关联（gateway 详设 8.2）。
const TraceIDKey = "x-trace-id"

// TraceID 返回调用方附带的 trace_id；没有时为空。
func TraceID(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if vals := md.Get(TraceIDKey); len(vals) == 1 {
		return vals[0]
	}
	return ""
}

// HasRole 判断 Principal 是否带有 roles 中的任一平台角色。管理接口实现方用它再校验一次（gateway 详设第 9 节的双重校验）。
func HasRole(p *identityv1.Principal, roles ...string) bool {
	for _, r := range p.GetRoles() {
		if slices.Contains(roles, r) {
			return true
		}
	}
	return false
}
