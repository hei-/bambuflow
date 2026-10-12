// Package embed 调用嵌入模型计算查询向量：Triton 的 KServe v2 HTTP 接口（开发与 CI 用接口相同的模拟服务）。
// 与入库侧 processing 的客户端协议一致，查询向量和文档向量必须出自同一模型版本。
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hei-/bambuflow/services/retrieval/internal/search"
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

// Embed 一次批量送入所有子查询（详设第 4 节第 6 步），返回各子查询的向量与模型版本（如 bge-m3@mock-hash-1）。
// 模型给学习型稀疏向量（输出名 sparse，每段文本一个“词元 ID → 权重”的 JSON）时一并返回，与稠密向量共用这一次调用（详设 6.2）。
func (c *Client) Embed(ctx context.Context, texts []string) ([]search.QueryVector, string, error) {
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
			Name  string          `json:"name"`
			Shape []int           `json:"shape"`
			Data  json.RawMessage `json:"data"`
		} `json:"outputs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, "", err
	}
	if len(out.Outputs) == 0 || out.ModelVersion == "" {
		return nil, "", fmt.Errorf("嵌入模型响应格式不符")
	}
	dense, sparse := out.Outputs[0], -1
	for i, o := range out.Outputs {
		switch o.Name {
		case "embedding":
			dense = o
		case "sparse":
			sparse = i
		}
	}
	var flat []float32
	if len(dense.Shape) != 2 || json.Unmarshal(dense.Data, &flat) != nil {
		return nil, "", fmt.Errorf("嵌入模型响应格式不符")
	}
	n, dim := dense.Shape[0], dense.Shape[1]
	if n != len(texts) || dim <= 0 || len(flat) != n*dim {
		return nil, "", fmt.Errorf("嵌入模型返回 %d×%d，请求 %d 条", n, dim, len(texts))
	}
	vecs := make([]search.QueryVector, n)
	for i := range vecs {
		vecs[i].Dense = flat[i*dim : (i+1)*dim]
	}
	if sparse >= 0 {
		var items []string
		if err := json.Unmarshal(out.Outputs[sparse].Data, &items); err != nil || len(items) != n {
			return nil, "", fmt.Errorf("稀疏向量格式不符")
		}
		for i, raw := range items {
			var w map[string]float32
			if err := json.Unmarshal([]byte(raw), &w); err != nil {
				return nil, "", fmt.Errorf("稀疏向量格式不符: %w", err)
			}
			vecs[i].Sparse = map[uint32]float32{}
			for k, v := range w {
				id, err := strconv.ParseUint(k, 10, 32)
				if err != nil {
					return nil, "", fmt.Errorf("稀疏向量的词元 ID %q 不合法", k)
				}
				if v > 0 {
					vecs[i].Sparse[uint32(id)] = v
				}
			}
		}
	}
	return vecs, out.ModelName + "@" + out.ModelVersion, nil
}
