// Package embed 调用嵌入模型计算查询向量：Triton 的 KServe v2 HTTP 接口（开发与 CI 用接口相同的模拟服务）。
// 与入库侧 processing 的客户端协议一致，查询向量和文档向量必须出自同一模型版本。
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Client 实现 search.Embedder。
type Client struct {
	url, model string
	http       *http.Client
}

func New(baseURL, model string) *Client {
	// 超时只兜底；实际上限由请求 context 的 deadline 决定
	return &Client{url: strings.TrimRight(baseURL, "/"), model: model, http: &http.Client{Timeout: 5 * time.Second}}
}

type tensor struct {
	Name     string `json:"name"`
	Shape    []int  `json:"shape"`
	Datatype string `json:"datatype"`
	Data     any    `json:"data"`
}

// Embed 一次批量送入所有子查询（详设第 4 节第 6 步），返回向量与模型版本（如 bge-m3@mock-hash-1）。
func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, string, error) {
	body, err := json.Marshal(map[string]any{
		"inputs": []tensor{{Name: "text", Shape: []int{len(texts)}, Datatype: "BYTES", Data: texts}},
	})
	if err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/v2/models/"+c.model+"/infer", bytes.NewReader(body))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("嵌入模型返回 %d", resp.StatusCode)
	}
	var out struct {
		ModelName    string `json:"model_name"`
		ModelVersion string `json:"model_version"`
		Outputs      []struct {
			Shape []int     `json:"shape"`
			Data  []float32 `json:"data"`
		} `json:"outputs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, "", err
	}
	if len(out.Outputs) != 1 || len(out.Outputs[0].Shape) != 2 || out.ModelVersion == "" {
		return nil, "", fmt.Errorf("嵌入模型响应格式不符")
	}
	n, dim := out.Outputs[0].Shape[0], out.Outputs[0].Shape[1]
	if n != len(texts) || dim <= 0 || len(out.Outputs[0].Data) != n*dim {
		return nil, "", fmt.Errorf("嵌入模型返回 %d×%d，请求 %d 条", n, dim, len(texts))
	}
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = out.Outputs[0].Data[i*dim : (i+1)*dim]
	}
	return vecs, out.ModelName + "@" + out.ModelVersion, nil
}
