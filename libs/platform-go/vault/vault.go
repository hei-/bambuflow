// Package vault 从 Vault 的 KV v2 引擎读取密钥（技术设计文档 8.4）。
//
// 读取用于启动和轮换时取签名密钥等配置；写入只用于平台自己生成的密钥（如 Webhook 签名密钥）。
// 不用 Vault 官方 SDK，它的依赖面远大于这两个 HTTP 调用。令牌优先从 VAULT_TOKEN_FILE（Vault Agent 写入的文件）读，其次 VAULT_TOKEN。
package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ErrNotFound 表示密钥不存在（或令牌无权读取，Vault 两种情况都返回 404 或 403，这里不区分）。
var ErrNotFound = errors.New("vault: 密钥不存在或无权读取")

type Client struct {
	addr  string
	token func() (string, error)
	http  *http.Client
}

// FromEnv 按 VAULT_ADDR、VAULT_TOKEN_FILE / VAULT_TOKEN 创建客户端。
func FromEnv() (*Client, error) {
	addr := os.Getenv("VAULT_ADDR")
	if addr == "" {
		return nil, errors.New("vault: 未设置 VAULT_ADDR")
	}
	if file := os.Getenv("VAULT_TOKEN_FILE"); file != "" {
		dir, name := splitPath(file)
		return New(addr, func() (string, error) {
			// 每次读取都重新读文件：Vault Agent 续期后会改写它
			b, err := fs.ReadFile(os.DirFS(dir), name)
			return strings.TrimSpace(string(b)), err
		}), nil
	}
	if tok := os.Getenv("VAULT_TOKEN"); tok != "" {
		return New(addr, func() (string, error) { return tok, nil }), nil
	}
	return nil, errors.New("vault: 未设置 VAULT_TOKEN_FILE 或 VAULT_TOKEN")
}

func splitPath(p string) (string, string) {
	i := strings.LastIndex(p, "/")
	if i < 0 {
		return ".", p
	}
	return p[:i+1], p[i+1:]
}

func New(addr string, token func() (string, error)) *Client {
	return &Client{addr: strings.TrimRight(addr, "/"), token: token, http: &http.Client{Timeout: 5 * time.Second}}
}

// ReadKV 读取 KV v2 引擎 mount 下 path 的最新版本，返回其中的键值。
func (c *Client) ReadKV(ctx context.Context, mount, path string) (map[string]string, error) {
	tok, err := c.token()
	if err != nil {
		return nil, fmt.Errorf("vault: 读取令牌: %w", err)
	}
	u := c.addr + "/v1/" + url.PathEscape(mount) + "/data/" + escapePath(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", tok)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusForbidden:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("vault: 读取 %s/%s 返回 %d", mount, path, resp.StatusCode)
	}
	var body struct {
		Data struct {
			Data map[string]string `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("vault: 解析响应: %w", err)
	}
	return body.Data.Data, nil
}

// WriteKV 在 KV v2 引擎 mount 下的 path 写入一个新版本（整份替换）。
func (c *Client) WriteKV(ctx context.Context, mount, path string, data map[string]string) error {
	tok, err := c.token()
	if err != nil {
		return fmt.Errorf("vault: 读取令牌: %w", err)
	}
	body, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return err
	}
	u := c.addr + "/v1/" + url.PathEscape(mount) + "/data/" + escapePath(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Vault-Token", tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("vault: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		// 不读响应体：Vault 的错误信息里可能回显请求内容
		return fmt.Errorf("vault: 写入 %s/%s 返回 %d", mount, path, resp.StatusCode)
	}
	return nil
}

func escapePath(p string) string {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}
