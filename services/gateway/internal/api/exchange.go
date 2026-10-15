package api

import (
	"context"
	"log/slog"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	identityv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/hei-/bambuflow/libs/platform-go/principal"
	"github.com/hei-/bambuflow/services/gateway/internal/issuer"
)

// PrincipalServer 实现 PrincipalService.Exchange（gateway 详设 3.3）：qa/batch 每条执行前凭提交时的委托换发 Principal。
// 只在内部 gRPC 端口提供；生产环境由服务网格的 mTLS 与网络策略限定只有 qa/batch 能连到（技术设计文档 12.6），
// 委托本身又绑定了批次与提交人，qa/batch 也只能代提交过批次的用户、在委托有效期内换发。
type PrincipalServer struct {
	identityv1.UnimplementedPrincipalServiceServer
	S *Server
}

// Exchange 按提交人当前的属性重新签发 Principal：提交人停用、不再是租户成员、租户不再对其开放问答、
// 委托过期或不属于该批次时一律 PERMISSION_DENIED，qa/batch 据此让剩余条目失败。
// 身份同步不新鲜时 UNAVAILABLE（可重试）：撤权时效无法保证，与在线请求同样处理。
func (p *PrincipalServer) Exchange(ctx context.Context, req *identityv1.ExchangeRequest) (*identityv1.ExchangeResponse, error) {
	s := p.S
	c := &call{ctx: ctx, traceID: newTraceID(), endpoint: "batch-exchange", start: s.Now(), batchID: req.GetBatchId()}
	resp, err := p.exchange(c, req)
	code := http.StatusOK
	reason := ""
	switch {
	case status.Code(err) == codes.PermissionDenied:
		code, reason = http.StatusForbidden, "forbidden"
	case err != nil:
		code, reason = http.StatusServiceUnavailable, "service_unavailable"
	}
	// 换发是批量任务以用户身份访问数据的入口，和在线请求一样每次都写审计；写不进去就不签发
	countRequest(c, code, reason)
	if aerr := s.writeAudit(c, code, reason, ""); aerr != nil {
		s.Log.ErrorContext(ctx, "入口审计写入失败", slog.Any("err", aerr))
		return nil, status.Error(codes.Unavailable, "审计不可用")
	}
	return resp, err
}

func (p *PrincipalServer) exchange(c *call, req *identityv1.ExchangeRequest) (*identityv1.ExchangeResponse, error) {
	s := p.S
	d, err := s.Issuer.VerifyDelegation(req.GetDelegation(), req.GetBatchId())
	if err != nil {
		s.Log.WarnContext(c.ctx, "批量委托无效", slog.String("batch_id", req.GetBatchId()), slog.Any("err", err))
		return nil, status.Error(codes.PermissionDenied, "委托无效")
	}
	c.sub, c.tenant, c.actor = d.Username, d.Tenant, d.Actor

	last, err := s.Identities.LastSync(c.ctx)
	if err != nil {
		s.Log.ErrorContext(c.ctx, "读取身份同步状态失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "身份库不可用")
	}
	if s.Now().Sub(last) > maxIdentityStaleness {
		s.Log.ErrorContext(c.ctx, "身份同步超过 15 分钟未成功，拒绝换发", slog.Time("last_success", last))
		return nil, status.Error(codes.Unavailable, "身份同步不新鲜")
	}
	user, ok, err := s.Identities.Lookup(c.ctx, d.Username)
	if err != nil {
		s.Log.ErrorContext(c.ctx, "读取平台身份库失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "身份库不可用")
	}
	if !ok || !user.Active {
		return nil, status.Error(codes.PermissionDenied, "提交人已停用")
	}
	// 按提交时的客户端与租户重新判定成员关系：用户被移出租户后，后续条目不再执行
	t, err := s.Tenants().Resolve(d.Actor, d.Tenant, user.Groups)
	if err != nil || t.Name != d.Tenant || !t.AnswersAllowed(user.Groups) {
		return nil, status.Error(codes.PermissionDenied, "不再是租户成员")
	}
	// 受限级数据禁止批量导出（技术设计文档 8.6）：批量条目的身份密级最高到机密，受限级文档在召回时就被过滤，
	// 清单类回答也不会列出；不在 orchestrator 或 qa/batch 里另判，多跳的检索调用目的不是 batch 也照样生效
	clearance := min(user.Clearance, int32(commonv1.Classification_CLASSIFICATION_CONFIDENTIAL))
	token, jti, err := s.Issuer.Issue(issuer.Subject{
		Username: user.Username, Tenant: t.Name, Dept: user.Dept, Title: user.Title, Region: user.Region,
		Clearance: clearance, Groups: user.Groups, Actor: d.Actor, AuthTime: d.AuthTime,
	})
	if err != nil {
		s.Log.ErrorContext(c.ctx, "签发 Principal 失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "签发失败")
	}
	c.jti = jti
	return &identityv1.ExchangeResponse{Principal: token, ExpiresAt: timestamppb.New(s.Now().Add(principal.MaxLifetime))}, nil
}
