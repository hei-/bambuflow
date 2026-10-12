package api

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/hei-/bambuflow/services/gateway/internal/authn"
)

// checkOnBehalf 核对业务系统代用户调用（gateway 详设 3.1）：令牌带 act 时（业务系统经平台授权服务做令牌交换取得），
// 请求还要带着同一业务系统的服务身份（mTLS 客户端证书），调用方须是登记到租户的业务系统（租户的 bound_clients，
// 由随后的租户解析核对）。没有 act 的令牌不受影响。
func (s *Server) checkOnBehalf(r *http.Request, c *call, caller authn.Caller) error {
	if caller.Act == "" {
		return nil
	}
	if s.Tenants().PlatformClient(caller.ClientID) {
		return problemForbidden
	}
	if id := s.serviceIdentity(r); id == "" || id != caller.Act {
		s.Log.InfoContext(c.ctx, "代用户调用的服务身份与令牌不符", slog.String("act", caller.Act), slog.Bool("cert", id != ""))
		return problemUnauth
	}
	return nil
}

// serviceIdentity 返回入口代理校验过的客户端证书主题里的 CN。入口代理（Ingress）终结 mTLS、校验证书后把主题 DN
// 写进 ClientCertHeader（如 ingress-nginx 的 ssl-client-subject-dn），并覆盖客户端自带的同名请求头；
// 网络策略只放行入口代理访问 gateway 的对外端口，所以这个头可信。未配置或主题里没有恰好一个 CN 时返回空。
func (s *Server) serviceIdentity(r *http.Request) string {
	if s.ClientCertHeader == "" {
		return ""
	}
	return commonName(r.Header.Get(s.ClientCertHeader))
}

// commonName 从 RFC 4514 格式的 DN（如 "CN=crm-app,O=Corp"）里取 CN；转义的逗号不拆。
func commonName(dn string) string {
	var cn []string
	var rdn strings.Builder
	flush := func() {
		attr, val, ok := strings.Cut(strings.TrimSpace(rdn.String()), "=")
		if ok && strings.EqualFold(strings.TrimSpace(attr), "CN") {
			cn = append(cn, strings.TrimSpace(val))
		}
		rdn.Reset()
	}
	escaped := false
	for _, ch := range dn {
		switch {
		case escaped:
			rdn.WriteRune(ch)
			escaped = false
		case ch == '\\':
			escaped = true
		case ch == ',' || ch == '+':
			flush()
		default:
			rdn.WriteRune(ch)
		}
	}
	flush()
	if len(cn) != 1 {
		return ""
	}
	return cn[0]
}
