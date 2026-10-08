// Package issuer 签发内部身份 Principal 并发布验签公钥（gateway 详设 3.2）。
package issuer

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
)

// MaxInlineGroups：群组超过这个数时 Principal 只放引用，由 retrieval 从平台身份库另取（K14）。
const MaxInlineGroups = 200

type Issuer struct {
	key *ecdsa.PrivateKey
	kid string
	now func() time.Time
}

// FromPEM 用 PKCS#8 PEM 格式的 P-256 私钥创建签发器；kid 取公钥摘要，轮换时新旧密钥自然区分。
func FromPEM(pemKey string) (*Issuer, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, errors.New("签名密钥不是 PEM 格式")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ec, ok := k.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() {
		return nil, errors.New("签名密钥必须是 P-256")
	}
	return New(ec)
}

func New(key *ecdsa.PrivateKey) (*Issuer, error) {
	pub, err := key.PublicKey.Bytes()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(pub)
	return &Issuer{key: key, kid: hex.EncodeToString(sum[:8]), now: time.Now}, nil
}

// Subject 是签发 Principal 所需的全部输入，都来自平台身份库与已校验的平台令牌。
type Subject struct {
	Username, Tenant, Dept, Title, Region, Actor string
	Clearance                                    int32
	Groups                                       []string
	AuthTime                                     int64
	// Roles 只用于管理接口的 Principal（IssueOperator）
	Roles []string
}

// Issue 签发数据接口的 Principal，返回令牌与 jti（入口审计用）。
func (i *Issuer) Issue(s Subject) (string, string, error) {
	if s.Tenant == "" || len(s.Roles) > 0 {
		return "", "", errors.New("数据接口的 Principal 必须有租户、不能带平台角色")
	}
	p := &identityv1.Principal{
		Sub: s.Username, Tenant: s.Tenant, Dept: s.Dept, Title: s.Title, Region: s.Region,
		Clearance: commonv1.Classification(s.Clearance), Actor: s.Actor, AuthTime: s.AuthTime,
	}
	if len(s.Groups) > MaxInlineGroups {
		p.GroupsRef = s.Username
	} else {
		p.Groups = s.Groups
	}
	return i.sign(p)
}

// IssueOperator 签发管理接口的 Principal：带平台角色、不带租户与群组（gateway 详设 4.2）。
// 部门要带上：数据属主只能操作本部门的数据源，实现方按它判断。
func (i *Issuer) IssueOperator(s Subject) (string, string, error) {
	if s.Tenant != "" || len(s.Roles) == 0 {
		return "", "", errors.New("管理接口的 Principal 必须有平台角色、不能带租户")
	}
	return i.sign(&identityv1.Principal{
		Sub: s.Username, Dept: s.Dept, Title: s.Title, Region: s.Region,
		Clearance: commonv1.Classification(s.Clearance), Actor: s.Actor, AuthTime: s.AuthTime, Roles: s.Roles,
	})
}

func (i *Issuer) sign(p *identityv1.Principal) (string, string, error) {
	jtiBytes := make([]byte, 16)
	if _, err := rand.Read(jtiBytes); err != nil {
		return "", "", err
	}
	now := i.now()
	p.Iat, p.Exp, p.Jti, p.Aud = now.Unix(), now.Add(principal.MaxLifetime).Unix(), hex.EncodeToString(jtiBytes), principal.Audience
	// 声明名与契约字段原名一致（contracts/bambuflow/identity/v1/principal.proto）
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(p)
	if err != nil {
		return "", "", err
	}
	var claims jwt.MapClaims
	if err := json.Unmarshal(data, &claims); err != nil {
		return "", "", err
	}
	claims["iss"] = principal.Issuer
	// protojson 把 int64 编成字符串，JWT 的时间声明必须是数字
	claims["iat"], claims["exp"], claims["auth_time"] = p.Iat, p.Exp, p.AuthTime
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	tok.Header["kid"] = i.kid
	signed, err := tok.SignedString(i.key)
	return signed, p.Jti, err
}

// JWKS 发布验签公钥，供下游服务（libs/platform-go/principal.JWKS）获取。只在内部端口提供。
func (i *Issuer) JWKS(w http.ResponseWriter, _ *http.Request) {
	pub, err := i.key.PublicKey.Bytes()
	if err != nil {
		http.Error(w, "内部错误", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "max-age=60")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
		"kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": i.kid,
		"x": base64.RawURLEncoding.EncodeToString(pub[1:33]),
		"y": base64.RawURLEncoding.EncodeToString(pub[33:]),
	}}})
}

func (i *Issuer) KID() string { return i.kid }
