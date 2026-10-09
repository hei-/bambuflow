// devseed 在开发与 CI 的 Vault（dev 模式）里写入 gateway 的 Principal 签名密钥、governance 的快照签名密钥，并启用 PII 原值信封加密用的
// transit 引擎与主密钥（技术设计文档 8.6）。只在测试工具镜像里存在。
//
// 只在密钥不存在时写入（KV v2 的 cas=0），重复运行不会换掉已有密钥。生产密钥由 Vault 管理员按轮换流程写入。
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "devseed:", err)
		os.Exit(1)
	}
}

// vaultClient 只做一件事：在 KV v2 中“不存在才写入”。地址与令牌来自部署环境。
type vaultClient struct {
	addr, token string
	http        *http.Client
}

func run() error {
	addr, token := os.Getenv("VAULT_ADDR"), os.Getenv("VAULT_TOKEN")
	if addr == "" || token == "" {
		return fmt.Errorf("需要 VAULT_ADDR 与 VAULT_TOKEN")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	c := &vaultClient{addr: addr, token: token, http: &http.Client{Timeout: 10 * time.Second}}
	created, err := c.createIfAbsent("secret", "gateway/principal-signing", map[string]string{"private_key": pemKey})
	if err != nil {
		return err
	}
	if created {
		fmt.Println("devseed: 已写入 gateway 签名密钥")
	} else {
		fmt.Println("devseed: 签名密钥已存在，保持不变")
	}
	// governance 的快照签名私钥（governance 详设 9.2）：与 Principal 签名密钥分开，泄露一个不影响另一个
	snapKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	snapDER, err := x509.MarshalPKCS8PrivateKey(snapKey)
	if err != nil {
		return err
	}
	snapPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: snapDER}))
	if _, err := c.createIfAbsent("secret", "governance/snapshot-signing", map[string]string{"private_key": snapPEM}); err != nil {
		return err
	}
	// PII 原值的信封加密：transit 引擎按需生成数据密钥并包裹，主密钥不离开 Vault。已存在时 Vault 返回 400，视为已就绪
	if err := c.post("sys/mounts/transit", map[string]any{"type": "transit"}, http.StatusNoContent, http.StatusOK, http.StatusBadRequest); err != nil {
		return err
	}
	for _, key := range []string{piiKey, answerKey} {
		if err := c.post("transit/keys/"+key, map[string]any{"type": "aes256-gcm96"}, http.StatusNoContent, http.StatusOK); err != nil {
			return err
		}
	}
	fmt.Println("devseed: PII 与回答记录的加密主密钥已就绪")
	return nil
}

// piiKey 是 PII 原值信封加密的主密钥名（processing 生成数据密钥、retrieval 解开，部署配置 BF_PII_KEY）。
const piiKey = "bambuflow-pii"

// answerKey 是回答记录（问题原文、受限级回答快照）信封加密的主密钥名（orchestrator 生成与解开，部署配置 BF_ANSWER_KEY）。
const answerKey = "bambuflow-answers"

func (c *vaultClient) post(path string, body map[string]any, ok ...int) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/v1/"+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("X-Vault-Token", c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	for _, code := range ok {
		if resp.StatusCode == code {
			return nil
		}
	}
	return fmt.Errorf("%s 返回 %d", path, resp.StatusCode)
}

func (c *vaultClient) createIfAbsent(mount, path string, data map[string]string) (bool, error) {
	body, err := json.Marshal(map[string]any{"options": map[string]int{"cas": 0}, "data": data})
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/v1/"+mount+"/data/"+path, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("X-Vault-Token", c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusBadRequest:
		// cas=0 且密钥已存在时 Vault 返回 400：保留已有密钥
		return false, nil
	default:
		return false, fmt.Errorf("写入 Vault 返回 %d", resp.StatusCode)
	}
}
