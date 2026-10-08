package issuer

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
)

const (
	// DelegationAudience 是批量任务委托的受众。与 Principal 的受众不同：下游按 Principal 校验时会拒绝它，
	// 委托只能拿回 gateway 换发，不能直接当身份用。
	DelegationAudience = "bambuflow-batch-delegation"
	// MaxDelegationLifetime 是委托的最长有效期（gateway 详设 3.3：不超过 7 天）。
	MaxDelegationLifetime = 7 * 24 * time.Hour
)

// ErrDelegationInvalid 表示委托无效（签名、受众、过期、批次不符）。具体原因只用于日志。
var ErrDelegationInvalid = errors.New("委托无效")

// Delegation 是提交批量任务时记下的委托（gateway 详设 3.3）：提交人、租户、发起提交的客户端、批次与有效期。
// 只记“谁在什么范围内授权”，不记用户属性：属性在每次换发时按平台身份库的当前值重新读取。
type Delegation struct {
	Username, Tenant, Actor, BatchID string
	AuthTime                         int64
	Expires                          time.Time
}

type delegationClaims struct {
	jwt.RegisteredClaims
	Tenant   string `json:"tenant"`
	Actor    string `json:"actor"`
	BatchID  string `json:"batch_id"`
	AuthTime int64  `json:"auth_time"`
}

// IssueDelegation 用签发 Principal 的同一把密钥签发委托；有效期取 d.Expires，不超过 7 天。
func (i *Issuer) IssueDelegation(d Delegation) (string, error) {
	now := i.now()
	if d.Username == "" || d.Tenant == "" || d.BatchID == "" || !d.Expires.After(now) || d.Expires.Sub(now) > MaxDelegationLifetime {
		return "", fmt.Errorf("%w: 字段不全或有效期越界", ErrDelegationInvalid)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, delegationClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: principal.Issuer, Subject: d.Username, Audience: jwt.ClaimStrings{DelegationAudience},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(d.Expires),
		},
		Tenant: d.Tenant, Actor: d.Actor, BatchID: d.BatchID, AuthTime: d.AuthTime,
	})
	tok.Header["kid"] = i.kid
	return tok.SignedString(i.key)
}

// VerifyDelegation 校验委托并核对它属于 batchID。只认本实例当前的签名密钥：
// 密钥轮换后，用旧密钥签发、还没执行完的批次会被拒绝（剩余条目失败），轮换时要保留旧密钥的验签能力至少 7 天。
func (i *Issuer) VerifyDelegation(raw, batchID string) (Delegation, error) {
	var c delegationClaims
	_, err := jwt.ParseWithClaims(raw, &c,
		func(t *jwt.Token) (any, error) {
			if kid, _ := t.Header["kid"].(string); kid != i.kid {
				return nil, errors.New("kid 不是当前密钥")
			}
			return &i.key.PublicKey, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}),
		jwt.WithIssuer(principal.Issuer),
		jwt.WithAudience(DelegationAudience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(i.now),
	)
	if err != nil {
		return Delegation{}, fmt.Errorf("%w: %w", ErrDelegationInvalid, err)
	}
	if c.IssuedAt == nil || c.ExpiresAt.Sub(c.IssuedAt.Time) > MaxDelegationLifetime {
		return Delegation{}, fmt.Errorf("%w: 有效期超过 %s", ErrDelegationInvalid, MaxDelegationLifetime)
	}
	if c.Subject == "" || c.Tenant == "" || c.BatchID == "" || c.BatchID != batchID {
		return Delegation{}, fmt.Errorf("%w: 批次不符或缺少字段", ErrDelegationInvalid)
	}
	return Delegation{Username: c.Subject, Tenant: c.Tenant, Actor: c.Actor, BatchID: c.BatchID,
		AuthTime: c.AuthTime, Expires: c.ExpiresAt.Time}, nil
}
