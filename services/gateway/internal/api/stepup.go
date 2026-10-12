package api

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hei-/bambuflow/services/gateway/internal/authn"
)

// StepUp 是审批类管理操作要求的加强认证（governance 详设 9.1：审批操作要求 IdP 的加强认证；部署配置清单 P7 默认强制）。
// 平台令牌的 acr 必须是 ACR 之一，且 auth_time 在 MaxAge 之内：单个账号被盗、或长期会话靠刷新令牌续着时，
// 光凭会话不能审批，必须当场重新认证。不满足时按 RFC 9470 返回 401 insufficient_user_authentication，
// console 据 WWW-Authenticate 里的 acr_values 与 max_age 发起重新登录。
// acr 的取值由平台授权服务配置（如 Keycloak 的认证强度映射：多因素登录给 "2"），部署时与 IAM 对齐。
type StepUp struct {
	ACR    []string
	MaxAge time.Duration
}

func (s StepUp) satisfied(c authn.Caller, now time.Time) bool {
	if c.AuthTime == 0 || !slices.Contains(s.ACR, c.ACR) {
		return false
	}
	return now.Sub(time.Unix(c.AuthTime, 0)) <= s.MaxAge
}

func (s StepUp) challenge() string {
	return fmt.Sprintf(`Bearer error="insufficient_user_authentication", error_description="审批操作需要重新认证", acr_values=%q, max_age=%d`,
		strings.Join(s.ACR, " "), int(s.MaxAge.Seconds()))
}
