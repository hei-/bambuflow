package pii

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// VaultTransit 是 KeyService 的默认实现：调用 Vault transit 的 decrypt 解开数据密钥，主密钥不离开 Vault。
// 令牌每次从文件读取（生产由 Vault Agent 写入并续期）；文件为空时用 Token。
type VaultTransit struct {
	Addr, Token, TokenFile string
	HTTP                   *http.Client
}

func (v *VaultTransit) token() (string, error) {
	if v.TokenFile == "" {
		return v.Token, nil
	}
	b, err := os.ReadFile(filepath.Clean(v.TokenFile))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func (v *VaultTransit) Unwrap(ctx context.Context, keyID, wrapped string) ([]byte, error) {
	tok, err := v.token()
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]string{"ciphertext": wrapped})
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(v.Addr, "/") + "/v1/transit/decrypt/" + keyID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", tok)
	resp, err := v.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("transit decrypt 返回 %d", resp.StatusCode)
	}
	var out struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(out.Data.Plaintext)
}
