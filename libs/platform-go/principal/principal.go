// Package principal 校验 gateway 签发的内部身份 Principal（gateway 详设 3.2）。
//
// Principal 是 ES256 签名的 JWT，放在 gRPC metadata 的 authorization 里。下游服务只信任它，
// 不再处理外部令牌；任何校验失败都按未认证处理（fail-closed）。
package principal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/encoding/protojson"

	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
)

const (
	// Issuer 是 gateway 签发 Principal 时使用的签发方。
	Issuer = "bambuflow-gateway"
	// Audience 是 Principal 的固定受众：只在平台内部使用，外部服务不接受。
	Audience = "bambuflow-internal"
	// MaxLifetime 是 Principal 的最长有效期。比它长的令牌即使签名正确也拒绝，防止签发端配置错误放大撤权延迟。
	MaxLifetime = 5 * time.Minute
)

// ErrInvalid 表示 Principal 无效。具体原因只用于日志，不返回给调用方。
var ErrInvalid = errors.New("principal 无效")

// KeySource 按 kid 提供验签公钥。
type KeySource interface {
	Key(ctx context.Context, kid string) (any, error)
}

// Verifier 校验 Principal 并解析出声明。
type Verifier struct {
	keys KeySource
	now  func() time.Time
	// operator 为真时只接受管理接口的 Principal（有平台角色、没有租户），否则只接受数据接口的 Principal
	operator bool
}

// NewVerifier 返回数据接口用的校验器：Principal 必须带租户、不带平台角色。
func NewVerifier(keys KeySource) *Verifier {
	return &Verifier{keys: keys, now: time.Now}
}

// NewOperatorVerifier 返回管理接口实现方用的校验器：Principal 必须带至少一个平台角色、不带租户
// （gateway 详设 4.2）。两类 Principal 互不通用：管理身份查不了数据，数据身份调不了管理接口。
func NewOperatorVerifier(keys KeySource) *Verifier {
	return &Verifier{keys: keys, now: time.Now, operator: true}
}

// Verify 校验签名、算法、签发方、受众与有效期，返回解析后的 Principal。
func (v *Verifier) Verify(ctx context.Context, raw string) (*identityv1.Principal, error) {
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(raw, claims,
		func(t *jwt.Token) (any, error) {
			kid, _ := t.Header["kid"].(string)
			if kid == "" {
				return nil, errors.New("缺少 kid")
			}
			return v.keys.Key(ctx, kid)
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}),
		jwt.WithIssuer(Issuer),
		jwt.WithAudience(Audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(v.now),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	p, err := toPrincipal(claims)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if p.GetIat() == 0 || time.Duration(p.GetExp()-p.GetIat())*time.Second > MaxLifetime {
		return nil, fmt.Errorf("%w: 有效期超过 %s", ErrInvalid, MaxLifetime)
	}
	switch {
	case p.GetSub() == "":
		return nil, fmt.Errorf("%w: 缺少 sub", ErrInvalid)
	case v.operator && (len(p.GetRoles()) == 0 || p.GetTenant() != ""):
		return nil, fmt.Errorf("%w: 不是管理接口的 Principal", ErrInvalid)
	case !v.operator && (p.GetTenant() == "" || len(p.GetRoles()) > 0):
		return nil, fmt.Errorf("%w: 不是数据接口的 Principal", ErrInvalid)
	}
	return p, nil
}

// toPrincipal 把 JWT 声明转成契约中的 Principal；声明名与字段原名一致（contracts/bambuflow/identity/v1）。
func toPrincipal(claims jwt.MapClaims) (*identityv1.Principal, error) {
	data, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}
	p := &identityv1.Principal{}
	// 声明里多出的字段（如 nbf）忽略；类型不符（如 aud 是数组）报错。
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, p); err != nil {
		return nil, fmt.Errorf("声明格式不符: %w", err)
	}
	return p, nil
}
