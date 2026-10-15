package backup

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

// Wrapper 包裹与解开数据密钥（主密钥不离开密钥服务）。
type Wrapper interface {
	Wrap(ctx context.Context, keys [][]byte) ([]string, error)
	Unwrap(ctx context.Context, wrapped []string) ([][]byte, error)
}

// Transit 是 Wrapper 的默认实现：Vault transit 的批量 encrypt / decrypt（batch_input），主密钥 Key（部署配置 BF_BACKUP_KEY）。
// 令牌每次从文件读（Vault Agent 写入）；文件为空时用 Token。
type Transit struct {
	Addr, Key, Token, TokenFile string
	HTTP                        *http.Client
}

// batchSize 是一次请求包裹或解开的密钥数
const batchSize = 500

func (t *Transit) Wrap(ctx context.Context, keys [][]byte) ([]string, error) {
	in := make([]map[string]string, len(keys))
	for i, k := range keys {
		in[i] = map[string]string{"plaintext": base64.StdEncoding.EncodeToString(k)}
	}
	out, err := t.batch(ctx, "encrypt", in, "ciphertext")
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (t *Transit) Unwrap(ctx context.Context, wrapped []string) ([][]byte, error) {
	in := make([]map[string]string, len(wrapped))
	for i, w := range wrapped {
		in[i] = map[string]string{"ciphertext": w}
	}
	out, err := t.batch(ctx, "decrypt", in, "plaintext")
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, len(out))
	for i, p := range out {
		if keys[i], err = base64.StdEncoding.DecodeString(p); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (t *Transit) batch(ctx context.Context, op string, items []map[string]string, field string) ([]string, error) {
	out := make([]string, 0, len(items))
	for start := 0; start < len(items); start += batchSize {
		part := items[start:min(start+batchSize, len(items))]
		got, err := t.call(ctx, op, part, field)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
	}
	return out, nil
}

func (t *Transit) call(ctx context.Context, op string, items []map[string]string, field string) ([]string, error) {
	tok := t.Token
	if t.TokenFile != "" {
		b, err := os.ReadFile(filepath.Clean(t.TokenFile))
		if err != nil {
			return nil, err
		}
		tok = strings.TrimSpace(string(b))
	}
	body, err := json.Marshal(map[string]any{"batch_input": items})
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(t.Addr, "/") + "/v1/transit/" + op + "/" + t.Key
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", tok)
	client := t.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("transit %s 返回 %d", op, resp.StatusCode)
	}
	// 每条结果还带 key_version 等数字字段，只取用到的三个
	var res struct {
		Data struct {
			BatchResults []struct {
				Ciphertext string `json:"ciphertext"`
				Plaintext  string `json:"plaintext"`
				Error      string `json:"error"`
			} `json:"batch_results"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	if len(res.Data.BatchResults) != len(items) {
		return nil, fmt.Errorf("transit %s 返回 %d 条结果，期望 %d 条", op, len(res.Data.BatchResults), len(items))
	}
	out := make([]string, len(items))
	for i, r := range res.Data.BatchResults {
		if r.Error != "" {
			return nil, fmt.Errorf("transit %s 第 %d 条失败: %s", op, i, r.Error)
		}
		out[i] = r.Plaintext
		if field == "ciphertext" {
			out[i] = r.Ciphertext
		}
	}
	return out, nil
}
