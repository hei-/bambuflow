// Package denyrule 是紧急拒绝规则备用通道上的签名规则（governance 详设 8.2，部署配置清单 P8）。
//
// governance 整体不可用时信息安全仍要能止血：规则除了经 governance 写进索引库的拒绝名单，还以签名规则的形式
// 走两条备用通道送到 retrieval——事件流的独立主题（Stream），与运维投放到 retrieval 配置卷的规则文件。
// 两条通道只能增加拒绝，retrieval 只接受验签通过的规则。签名密钥与快照签名密钥分开（Vault 的 KeyPath），
// retrieval 只拿公钥，不依赖 governance 在线。
package denyrule

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// Stream 是事件流上的独立主题：只放签名规则，与平台事件流分开
	Stream = "bambuflow.emergency-denials"
	// KeyPath 是签名密钥在 Vault KV 里的位置：private_key（PKCS#8 PEM）只给 governance 与信息安全，public_key（PKIX PEM）给 retrieval
	KeyPath = "governance/emergency-signing"
)

// Rule 是一条拒绝规则。Scope 为 source、document 或 tenant，与 governance 的紧急拒绝规则一致。
type Rule struct {
	ID       string    `json:"id"`
	Scope    string    `json:"scope"`
	Target   string    `json:"target"`
	Reason   string    `json:"reason"`
	IssuedBy string    `json:"issued_by"`
	IssuedAt time.Time `json:"issued_at"`
}

// envelope 是签名后的规则：payload 是规则的 JSON，签名是 ECDSA P-256（ASN.1 DER）over SHA-256(payload)。
type envelope struct {
	Payload   []byte `json:"payload"`
	Signature []byte `json:"signature"`
	KeyID     string `json:"key_id"`
}

var (
	sourceID = regexp.MustCompile(`^src_[A-Za-z0-9]{1,64}$`)
	tenant   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	ruleID   = regexp.MustCompile(`^den_[A-Za-z0-9_-]{1,64}$`)
)

// Validate 检查规则字段（与 governance 登记规则时的校验一致）。
func (r Rule) Validate() error {
	ok := false
	switch r.Scope {
	case "source":
		ok = sourceID.MatchString(r.Target)
	case "tenant":
		ok = tenant.MatchString(r.Target)
	case "document":
		ok = r.Target != "" && len(r.Target) <= 512 && utf8.ValidString(r.Target) && !strings.ContainsFunc(r.Target, unicode.IsControl)
	}
	if !ok || !ruleID.MatchString(r.ID) || strings.TrimSpace(r.Reason) == "" || r.IssuedBy == "" || r.IssuedAt.IsZero() {
		return errors.New("规则字段不合法")
	}
	return nil
}

// KeyID 是公钥 PKIX DER 的 SHA-256 前 8 字节。
func KeyID(pub *ecdsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:8]), nil
}

// Sign 签名一条规则，返回可以放进事件流或写成文件的字节。
func Sign(key *ecdsa.PrivateKey, r Rule) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(payload)
	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		return nil, err
	}
	id, err := KeyID(&key.PublicKey)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{Payload: payload, Signature: sig, KeyID: id})
}

// Verifier 按 key_id 用受信任的公钥验签。
type Verifier struct {
	keys map[string]*ecdsa.PublicKey
}

func NewVerifier(keys ...*ecdsa.PublicKey) (*Verifier, error) {
	v := &Verifier{keys: map[string]*ecdsa.PublicKey{}}
	for _, k := range keys {
		id, err := KeyID(k)
		if err != nil {
			return nil, err
		}
		v.keys[id] = k
	}
	return v, nil
}

// Verify 验签并返回规则；签名不对、密钥不受信任或字段不合法都返回错误。
func (v *Verifier) Verify(data []byte) (Rule, error) {
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Rule{}, fmt.Errorf("不是签名规则: %w", err)
	}
	pub, ok := v.keys[e.KeyID]
	if !ok {
		return Rule{}, fmt.Errorf("签名密钥 %q 不受信任", e.KeyID)
	}
	digest := sha256.Sum256(e.Payload)
	if !ecdsa.VerifyASN1(pub, digest[:], e.Signature) {
		return Rule{}, errors.New("签名不对")
	}
	var r Rule
	if err := json.Unmarshal(e.Payload, &r); err != nil {
		return Rule{}, err
	}
	return r, r.Validate()
}

// ParsePrivateKey 解析 PKCS#8 PEM 格式的 P-256 私钥。
func ParsePrivateKey(pemKey string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, errors.New("签名密钥不是 PEM 格式")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ec, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("签名密钥不是 ECDSA")
	}
	return ec, nil
}

// ParsePublicKey 解析 PKIX PEM 格式的公钥。
func ParsePublicKey(pemKey string) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, errors.New("公钥不是 PEM 格式")
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ec, ok := k.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("公钥不是 ECDSA")
	}
	return ec, nil
}
