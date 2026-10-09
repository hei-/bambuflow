package vocab

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// Signer 给快照签名（governance 详设 9.2）：私钥来自 Vault，只在 governance 进程内存里。
// 签名是 ECDSA P-256（ASN.1 DER）over SHA-256(signed_payload)；key_id 是公钥 DER 的 SHA-256 前 16 位。
type Signer struct {
	key   *ecdsa.PrivateKey
	keyID string
	pub   []byte
}

// SecretReader 读 Vault KV（libs/platform-go/vault）。
type SecretReader interface {
	ReadKV(ctx context.Context, mount, path string) (map[string]string, error)
}

// SigningKeyPath 是快照签名私钥在 Vault KV 里的位置（字段 private_key，PKCS#8 PEM）。
const SigningKeyPath = "governance/snapshot-signing"

// LoadSigner 从 Vault 读签名私钥；读不到就不启动（没有签名的快照不能下发）。
func LoadSigner(ctx context.Context, v SecretReader, mount string) (*Signer, error) {
	data, err := v.ReadKV(ctx, mount, SigningKeyPath)
	if err != nil {
		return nil, fmt.Errorf("读取快照签名密钥: %w", err)
	}
	block, _ := pem.Decode([]byte(data["private_key"]))
	if block == nil {
		return nil, errors.New("快照签名密钥不是 PEM")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ec, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("快照签名密钥不是 ECDSA")
	}
	return NewSigner(ec)
}

// NewSigner 用给定的私钥签名（测试也用它）。
func NewSigner(key *ecdsa.PrivateKey) (*Signer, error) {
	pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(pub)
	return &Signer{key: key, keyID: hex.EncodeToString(sum[:8]), pub: pub}, nil
}

// Sign 把快照本体序列化进 signed_payload 并签名。
func (s *Signer) Sign(resp *configv1.GetSnapshotResponse, payload []byte) error {
	digest := sha256.Sum256(payload)
	sig, err := ecdsa.SignASN1(rand.Reader, s.key, digest[:])
	if err != nil {
		return err
	}
	resp.SignedPayload, resp.Signature, resp.SigningKeyId = payload, sig, s.keyID
	return nil
}

// Keys 是验签用的公钥列表。
func (s *Signer) Keys() []*configv1.SigningKey {
	return []*configv1.SigningKey{{KeyId: s.keyID, PublicKey: s.pub}}
}
