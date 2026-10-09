package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/audit"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/authn"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/idempotency"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/issuer"
)

// 管理接口（技术设计文档 13.2，gateway 详设 4.2）。业务逻辑由拥有数据的模块实现，gateway 只做三件事：
// 按登记的路由表分发（未登记的方法与路径一律 404，不转发到任何“默认后端”）、按路径和方法做 RBAC、写管理审计。
// 审批、双人复核、属主范围等业务规则由实现方再校验一次（gateway 详设第 9 节的双重校验）。
//
// 调用者必须是平台级客户端（如 console）上的用户；角色来自平台身份库的群组（internal/roles），
// 下游收到的是只带平台角色、不带租户的管理 Principal，数据接口不接受它。

// adminRoute 是路由表里的一项。
type adminRoute struct {
	// name 写进入口审计的 endpoint
	name string
	// method 与 pattern：pattern 的 {x} 匹配一个非空路径段，取值放在 adminInput.params
	method, pattern string
	// owner 是实现方（governance、indexing、evaluation、processing、connectors），写进管理审计
	owner string
	// roles 是允许调用的平台角色，有其一即可
	roles    []string
	deadline time.Duration
	call     func(ctx context.Context, in *adminInput) (adminOutput, error)
}

type adminInput struct {
	params map[string]string
	query  url.Values
	body   []byte
	// sub 是调用者，实现方用它判断“发起人不能自批”；gateway 不做这类判断
	sub string
}

// adminOutput 是实现方的结果：status 为 0 时按 200 返回。
type adminOutput struct {
	status int
	body   any
}

const (
	adminDeadline  = 5 * time.Second
	maxAdminKeyLen = 128
)

var (
	problemAdminKey = problem{http.StatusBadRequest, "invalid_request", "管理写操作必须带 Idempotency-Key（不超过 128 个字符）"}
	// 实现方用 grpcmw.Reason 返回的业务错误码（governance 详设 7.2），登记过的才原样对外
	adminReasons = map[string]problem{
		"self_approval_forbidden": {http.StatusForbidden, "self_approval_forbidden", "不能审批自己发起的操作"},
		"out_of_scope":            {http.StatusForbidden, "forbidden", "无权操作该对象"},
		"conflict":                {http.StatusConflict, "conflict", "对象的当前状态不允许这个操作"},
		"activation_refused":      {http.StatusConflict, "activation_refused", "目标版本不能上线：清单不全或嵌入模型已下线"},
		"stale_base_version":      {http.StatusConflict, "stale_base_version", "生效版本已经变了，请基于最新版本重新提交"},
		"gate_failed":             {http.StatusConflict, "gate_failed", "评测门禁不满足：须附已通过、覆盖该租户的门禁"},
	}
)

// match 按方法与路径找路由；路径段数不同、字面段不同或方法不同都不算匹配。
func match(routes []adminRoute, method, path string) (*adminRoute, map[string]string) {
	segs := strings.Split(strings.TrimSuffix(path, "/"), "/")
	for i := range routes {
		rt := &routes[i]
		if rt.method != method {
			continue
		}
		pat := strings.Split(rt.pattern, "/")
		if len(pat) != len(segs) {
			continue
		}
		params := map[string]string{}
		ok := true
		for j, p := range pat {
			if strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}") {
				if segs[j] == "" {
					ok = false
					break
				}
				params[p[1:len(p)-1]] = segs[j]
			} else if p != segs[j] {
				ok = false
				break
			}
		}
		if ok {
			return rt, params
		}
	}
	return nil, nil
}

// admin 处理 /v1/admin/ 下的全部请求。
func (s *Server) admin(w http.ResponseWriter, r *http.Request, c *call) (any, error) {
	routes := s.adminTable
	if routes == nil {
		routes = s.adminRoutes()
	}
	rt, params := match(routes, r.Method, r.URL.Path)
	if rt == nil {
		return nil, problemNotFound
	}
	c.endpoint = rt.name
	write := r.Method != http.MethodGet
	c.admin = &audit.Admin{Owner: rt.owner, Method: r.Method, Path: r.URL.Path}

	operator, err := s.authorizeAdmin(r, c, rt)
	if err != nil {
		return nil, err
	}
	in := &adminInput{params: params, query: r.URL.Query(), sub: c.sub}
	if write {
		if in.body, err = io.ReadAll(r.Body); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				return nil, problemTooLarge
			}
			return nil, problemInvalid
		}
		sum := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), in.body...))
		c.admin.RequestSHA, c.admin.Request = hex.EncodeToString(sum[:]), audit.Summarize(in.body)
		key := r.Header.Get("Idempotency-Key")
		if key == "" || len(key) > maxAdminKeyLen {
			return nil, problemAdminKey
		}
		c.admin.IdempotencyKey = key
		done, err := s.Idempotency.Begin(c.ctx, c.sub, key, c.admin.RequestSHA)
		switch {
		case errors.Is(err, idempotency.ErrConflict):
			return nil, problemIdempotency
		case err != nil:
			return nil, fmt.Errorf("幂等键登记: %w", err)
		case done != nil:
			c.admin.Replayed, c.admin.Result, c.httpStatus = true, audit.Summarize(done.Body), done.Status
			return done.Body, nil
		}
	}

	out, err := s.callAdmin(c, rt, operator, in)
	if !write {
		return out.body, err
	}
	if err != nil {
		// 失败的请求不占用幂等键：调用方可以用同一个键重试
		if aerr := s.Idempotency.Abort(context.WithoutCancel(c.ctx), c.sub, c.admin.IdempotencyKey); aerr != nil {
			s.Log.ErrorContext(c.ctx, "删除幂等键登记失败", slog.Any("err", aerr))
		}
		return nil, err
	}
	body, err := json.Marshal(out.body)
	if err != nil {
		return nil, err
	}
	c.admin.Result = audit.Summarize(body)
	if err := s.Idempotency.Finish(context.WithoutCancel(c.ctx), c.sub, c.admin.IdempotencyKey, c.admin.RequestSHA,
		idempotency.Result{Status: c.httpStatus, Body: body}); err != nil {
		// 操作已经执行：照常返回结果，只是同键重试会被当成冲突，不会重复执行
		s.Log.ErrorContext(c.ctx, "记录幂等结果失败", slog.Any("err", err))
	}
	return json.RawMessage(body), nil
}

// callAdmin 带上管理 Principal、入口 trace_id 与 deadline 调实现方，并把错误转成对外错误。
func (s *Server) callAdmin(c *call, rt *adminRoute, operator string, in *adminInput) (adminOutput, error) {
	d := rt.deadline
	if d == 0 {
		d = adminDeadline
	}
	ctx, cancel := context.WithTimeout(c.ctx, d)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+operator, grpcmw.TraceIDKey, c.traceID)
	out, err := rt.call(ctx, in)
	if err != nil {
		var p problem
		if errors.As(err, &p) {
			return adminOutput{}, p
		}
		if p, ok := adminReasons[grpcmw.ReasonOf(err)]; ok {
			return adminOutput{}, p
		}
		return adminOutput{}, fromGRPC(err)
	}
	if out.status == 0 {
		out.status = http.StatusOK
	}
	c.httpStatus = out.status
	return out, nil
}

// authorizeAdmin 认证、限流、读身份、映射角色并按路由表做 RBAC，返回签好的管理 Principal。
// 管理接口优先级最高，不经全局自适应并发（gateway 详设 5.2）。
func (s *Server) authorizeAdmin(r *http.Request, c *call, rt *adminRoute) (string, error) {
	caller, err := s.Verifier.Verify(c.ctx, r.Header.Get("Authorization"))
	switch {
	case errors.Is(err, authn.ErrUserIdentityRequired):
		return "", problemUserRequired
	case err != nil:
		s.Log.InfoContext(c.ctx, "平台令牌校验失败", slog.Any("err", err))
		return "", problemUnauth
	}
	c.sub, c.actor, c.authTime = caller.Username, caller.ClientID, caller.AuthTime
	if !s.Tenants().PlatformClient(caller.ClientID) {
		return "", problemForbidden
	}
	if !s.Limits.Allow(c.ctx, s.ClientRule, caller.ClientID) {
		return "", problemRateLimited
	}
	last, err := s.Identities.LastSync(c.ctx)
	if err != nil {
		return "", fmt.Errorf("读取身份同步状态: %w", err)
	}
	if s.Now().Sub(last) > maxIdentityStaleness {
		s.Log.ErrorContext(c.ctx, "身份同步超过 15 分钟未成功，拒绝请求", slog.Time("last_success", last))
		return "", problemUnavailable
	}
	user, ok, err := s.Identities.Lookup(c.ctx, caller.Username)
	if err != nil {
		return "", fmt.Errorf("读取平台身份库: %w", err)
	}
	if !ok || !user.Active {
		return "", problemForbidden
	}
	roles := s.AdminRoles.Of(user.Groups)
	c.admin.Roles = roles
	if !slices.ContainsFunc(roles, func(x string) bool { return slices.Contains(rt.roles, x) }) {
		return "", problemForbidden
	}
	token, jti, err := s.Issuer.IssueOperator(issuer.Subject{
		Username: user.Username, Dept: user.Dept, Title: user.Title, Region: user.Region, Clearance: user.Clearance,
		Actor: caller.ClientID, AuthTime: caller.AuthTime, Roles: roles,
	})
	if err != nil {
		return "", fmt.Errorf("签发管理 Principal: %w", err)
	}
	c.jti = jti
	return token, nil
}

// decodeBody 按对外字段解析管理请求体：未知字段、多余内容都按参数错误处理。空请求体视为 {}。
func decodeBody(body []byte, v any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return problemInvalid
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return problemInvalid
	}
	return nil
}

// rfc3339 把契约里的时间转成对外的 RFC 3339（UTC）；为空时返回空串。
func rfc3339(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}
