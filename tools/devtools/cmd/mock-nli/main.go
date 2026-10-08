// mock-nli 在没有 GPU 与真实 NLI 模型时顶替 Triton 上的 NLI 模型，只用于开发与 CI（P0 计划第 7 步）。
//
// 实现 KServe v2 HTTP 接口的子集，grounding 接真实 Triton 时代码相同，只换地址。支持度用“假设句的字符二元组
// 有多少出现在前提里”近似：照抄引文得 1，与引文无关的句子接近 0。只够验证校验链路，语义判断以真实模型为准。
package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
)

const (
	model   = "nli"
	version = "mock-bigram-1"
)

// bigrams 只取汉字、字母和数字，忽略标点、空白与引用标记的影响。
func bigrams(s string) map[string]bool {
	var rs []rune
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			rs = append(rs, r)
		}
	}
	out := map[string]bool{}
	for i := 0; i+1 < len(rs); i++ {
		out[string(rs[i:i+2])] = true
	}
	return out
}

func support(premise, hypothesis string) float32 {
	h := bigrams(hypothesis)
	if len(h) == 0 {
		return 0
	}
	p := bigrams(premise)
	hit := 0
	for b := range h {
		if p[b] {
			hit++
		}
	}
	return float32(hit) / float32(len(h))
}

type tensor struct {
	Name     string   `json:"name"`
	Shape    []int    `json:"shape"`
	Datatype string   `json:"datatype"`
	Data     []string `json:"data"`
}

func main() {
	log := logging.New(os.Stdout, "mock-nli", slog.LevelInfo)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/health/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /v2/models/"+model, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"name": model, "versions": []string{version}, "platform": "mock"})
	})
	mux.HandleFunc("POST /v2/models/"+model+"/infer", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Inputs []tensor `json:"inputs"`
		}
		bad := func() {
			http.Error(w, `{"error":"需要等长的 BYTES 输入 premise 与 hypothesis"}`, http.StatusBadRequest)
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
			bad()
			return
		}
		in := map[string][]string{}
		for _, t := range req.Inputs {
			if t.Datatype == "BYTES" {
				in[t.Name] = t.Data
			}
		}
		premises, hypotheses := in["premise"], in["hypothesis"]
		if len(premises) == 0 || len(premises) != len(hypotheses) {
			bad()
			return
		}
		scores := make([]float32, len(premises))
		for i := range premises {
			scores[i] = support(premises[i], hypotheses[i])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model_name": model, "model_version": version,
			"outputs": []map[string]any{{"name": "support", "datatype": "FP32", "shape": []int{len(scores)}, "data": scores}},
		})
	})
	addr := os.Getenv("BF_LISTEN")
	if addr == "" {
		addr = ":8000"
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("mock-nli 已启动")
	if err := srv.ListenAndServe(); err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}
