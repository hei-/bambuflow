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

// Reranker 调用交叉编码器精排模型（Triton 的 KServe v2 接口；开发用模型服务与模拟服务接口相同）。
type Reranker struct {
	url, model string
	http       *http.Client
}

func NewReranker(baseURL, model string) *Reranker {
	return &Reranker{url: strings.TrimRight(baseURL, "/"), model: model, http: &http.Client{Timeout: 5 * time.Second}}
}

// Rerank 一次批量送入全部（查询，段落）对（详设 7.4：首轮 50 对一次调用），返回 0–1 的分数与模型版本。
func (c *Reranker) Rerank(ctx context.Context, queries, passages []string) ([]float64, string, error) {
	body, err := json.Marshal(map[string]any{"inputs": []tensor{
		{Name: "query", Shape: []int{len(queries)}, Datatype: "BYTES", Data: queries},
		{Name: "passage", Shape: []int{len(passages)}, Datatype: "BYTES", Data: passages},
	}})
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
		return nil, "", fmt.Errorf("精排模型返回 %d", resp.StatusCode)
	}
	var out struct {
		ModelName    string `json:"model_name"`
		ModelVersion string `json:"model_version"`
		Outputs      []struct {
			Data []float64 `json:"data"`
		} `json:"outputs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, "", err
	}
	if len(out.Outputs) != 1 || len(out.Outputs[0].Data) != len(queries) || out.ModelVersion == "" {
		return nil, "", fmt.Errorf("精排模型响应格式不符")
	}
	return out.Outputs[0].Data, out.ModelName + "@" + out.ModelVersion, nil
}
