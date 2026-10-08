// Package authn 校验平台授权服务签发的访问令牌（gateway 详设 3.1、3.4）。gateway 只信任这一个签发方。
package authn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
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
}

// Verify 从 Authorization 头的值校验令牌并返回调用者。
func (v *Verifier) Verify(ctx context.Context, authorization string) (Caller, error) {
	raw, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || raw == "" {
		return Caller{}, fmt.Errorf("%w: 缺少 Bearer 令牌", ErrUnauthenticated)
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
		return Caller{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	if c.IssuedAt == nil || c.ExpiresAt.Sub(c.IssuedAt.Time) > MaxLifetime {
		return Caller{}, fmt.Errorf("%w: 有效期超过 %s", ErrUnauthenticated, MaxLifetime)
	}
	if c.AZP == "" {
		return Caller{}, fmt.Errorf("%w: 缺少 azp", ErrUnauthenticated)
	}
	if c.PreferredUsername == "" || strings.HasPrefix(c.PreferredUsername, v.serviceAccountPrefix) {
		return Caller{}, ErrUserIdentityRequired
	}
	return Caller{Username: c.PreferredUsername, ClientID: c.AZP, AuthTime: c.AuthTime}, nil
}
