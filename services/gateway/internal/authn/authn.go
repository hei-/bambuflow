// Package authn 校验平台授权服务签发的访问令牌（gateway 详设 3.1、3.4）。gateway 只信任这一个签发方。
package authn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/hei-/bambuflow/libs/platform-go/principal"
)

const (
	// Audience 是平台令牌的受众（config/authz 中 console 等客户端的受众映射）。
	Audience = "bambuflow-platform"
	// MaxLifetime 是平台令牌的最长有效期（ADR-13：15 分钟）。
	MaxLifetime = 15 * time.Minute
)

var (
	// ErrUnauthenticated：令牌缺失、过期、签名或签发方不对。
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrUserIdentityRequired：只有服务身份、没有终端用户身份。
	ErrUserIdentityRequired = errors.New("user_identity_required")
)

// Caller 是令牌证明的调用者。
type Caller struct {
	Username string // 终端用户在公司目录中的账号
	ClientID string // 发起调用的客户端（console、业务系统、agent）
	AuthTime int64
	// ACR 是这次登录的认证强度（OIDC acr），审批类管理操作按它判断是否做过加强认证
	ACR string
	// Act 是代用户调用的业务系统（RFC 8693 的 act.sub）：业务系统经平台授权服务做令牌交换取得的令牌才有，
	// 与 ClientID 相同；gateway 另要求请求的服务身份（mTLS）与它一致（gateway 详设 3.1）
	Act string
}

type Verifier struct {
	keys principal.KeySource
	// issuer 是平台 realm 的签发方地址，如 http://keycloak:8080/realms/bambuflow
	issuer string
	// serviceAccountPrefix 是平台授权服务给服务账号起的用户名前缀（Keycloak 为 "service-account-"）
	serviceAccountPrefix string
	now                  func() time.Time
}

func NewVerifier(keys principal.KeySource, issuer, serviceAccountPrefix string) *Verifier {
	return &Verifier{keys: keys, issuer: issuer, serviceAccountPrefix: serviceAccountPrefix, now: time.Now}
}

type claims struct {
	jwt.RegisteredClaims
	PreferredUsername string `json:"preferred_username"`
	AZP               string `json:"azp"`
	AuthTime          int64  `json:"auth_time"`
	ACR               string `json:"acr"`
	Act               *struct {
		Sub string `json:"sub"`
	} `json:"act"`
}

// Verify 从 Authorization 头的值校验令牌并返回调用者。
func (v *Verifier) Verify(ctx context.Context, authorization string) (Caller, error) {
	c, err := v.parse(ctx, authorization)
	if err != nil {
		return Caller{}, err
	}
	if c.PreferredUsername == "" || strings.HasPrefix(c.PreferredUsername, v.serviceAccountPrefix) {
		return Caller{}, ErrUserIdentityRequired
	}
	caller := Caller{Username: c.PreferredUsername, ClientID: c.AZP, AuthTime: c.AuthTime, ACR: c.ACR}
	if c.Act != nil {
		// 交换得到的令牌由平台授权服务给发起交换的客户端签发，act 就是它自己；对不上说明令牌不是这样来的
		if c.Act.Sub == "" || c.Act.Sub != c.AZP {
			return Caller{}, fmt.Errorf("%w: act 与 azp 不一致", ErrUnauthenticated)
		}
		caller.Act = c.Act.Sub
	}
	return caller, nil
}

// ErrServiceIdentityRequired：接口只接受服务身份（客户端凭证），带了终端用户的令牌。
var ErrServiceIdentityRequired = errors.New("service_identity_required")

// VerifyService 校验服务账号的令牌（客户端凭证模式），返回客户端 ID。推送入库这类系统对系统的接口用它：
// 不接受代表终端用户的令牌，免得用户借业务系统的客户端绕过数据源的推送登记。
func (v *Verifier) VerifyService(ctx context.Context, authorization string) (string, error) {
	c, err := v.parse(ctx, authorization)
	if err != nil {
		return "", err
	}
	if c.Act != nil || (c.PreferredUsername != "" && !strings.HasPrefix(c.PreferredUsername, v.serviceAccountPrefix)) {
		return "", ErrServiceIdentityRequired
	}
	return c.AZP, nil
}

// parse 校验签名、签发方、受众、有效期与 azp，返回声明。
func (v *Verifier) parse(ctx context.Context, authorization string) (claims, error) {
	raw, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || raw == "" {
		return claims{}, fmt.Errorf("%w: 缺少 Bearer 令牌", ErrUnauthenticated)
	}
	var c claims
	_, err := jwt.ParseWithClaims(raw, &c,
		func(t *jwt.Token) (any, error) {
			kid, _ := t.Header["kid"].(string)
			if kid == "" {
				return nil, errors.New("缺少 kid")
			}
			return v.keys.Key(ctx, kid)
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(Audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(v.now),
	)
	if err != nil {
		return claims{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	if c.IssuedAt == nil || c.ExpiresAt.Sub(c.IssuedAt.Time) > MaxLifetime {
		return claims{}, fmt.Errorf("%w: 有效期超过 %s", ErrUnauthenticated, MaxLifetime)
	}
	if c.AZP == "" {
		return claims{}, fmt.Errorf("%w: 缺少 azp", ErrUnauthenticated)
	}
	return c, nil
}
