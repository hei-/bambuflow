// mock-embedder 在没有 GPU 与真实模型时顶替 Triton 的嵌入模型，只用于开发与 CI（P0 计划第 4 节 #2）。
//
// 实现 Triton 的 KServe v2 HTTP 接口的子集，调用方（processing、retrieval）与接真实 Triton 时代码相同，只换地址。
// 向量用字符一元、二元组的哈希累加再归一化：确定、无需模型，字面相近的文本向量也相近，足以验证检索链路；
// 语义质量以 models profile 的真实模型为准。
package main

import (
	"encoding/json"
	"hash/fnv"
	"log/slog"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hei-/bambuflow/libs/platform-go/logging"
)

const (
	dim     = 1024
	model   = "bge-m3"
	version = "mock-hash-1"
	// 模拟精排模型的名字与真实模型相同，调用方换地址即可
	rerankModel = "bge-reranker-v2-m3"
)

// overlap 是查询的字二元组出现在段落里的比例，0–1。
func overlap(query, passage string) float64 {
	grams := func(s string) map[string]bool {
		var rs []rune
		for _, r := range strings.ToLower(s) {
			if r != ' ' && r != '\n' && r != '\t' {
				rs = append(rs, r)
			}
		}
		out := map[string]bool{}
		for i := 0; i+1 < len(rs); i++ {
			out[string(rs[i:i+2])] = true
		}
		return out
	}
	q, p := grams(query), grams(passage)
	if len(q) == 0 {
		return 0
	}
	hit := 0
	for g := range q {
		if p[g] {
			hit++
		}
	}
	return float64(hit) / float64(len(q))
}

func embed(text string) []float32 {
	v := make([]float64, dim)
	runes := []rune(strings.ToLower(text))
	add := func(s string) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(s))
		x := h.Sum64()
		sign := 1.0
		if x&(1<<63) != 0 {
			sign = -1
		}
		v[x%dim] += sign
	}
	for i, r := range runes {
		if r == ' ' || r == '\n' || r == '\t' {
			continue
		}
		add(string(r))
		if i+1 < len(runes) {
			add(string(runes[i : i+2]))
		}
	}
	var norm float64
	for _, x := range v {
		norm += x * x
	}
	out := make([]float32, dim)
	if norm == 0 {
		return out
	}
	norm = math.Sqrt(norm)
	for i, x := range v {
		out[i] = float32(x / norm)
	}
	return out
}

// SparseVocab 是模拟稀疏向量的词表大小（与 bge-m3 的词表一致，indexing 的 sparsevec 维数）。
const SparseVocab = 250002

// maxSparse 是一段文本的稀疏向量最多保留的词元数：pgvector 的 HNSW 只接受不超过 1000 个非零元素
const maxSparse = 256

// sparse 是模拟的学习型稀疏向量：字一元、二元组哈希到词表里的 ID，权重为 log(1 + 次数)，按权重留前 maxSparse 个。
// 与真实的 bge-m3 词权重一样以“词元 ID → 权重”表示，调用方的处理代码相同。
func sparse(text string) map[string]float32 {
	counts := map[uint32]int{}
	runes := []rune(strings.ToLower(text))
	add := func(s string) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(s))
		counts[h.Sum32()%SparseVocab]++
	}
	for i, r := range runes {
		if r == ' ' || r == '\n' || r == '\t' {
			continue
		}
		add(string(r))
		if i+1 < len(runes) {
			add(string(runes[i : i+2]))
		}
	}
	type kv struct {
		id uint32
		w  float32
	}
	all := make([]kv, 0, len(counts))
	for id, n := range counts {
		all = append(all, kv{id, float32(math.Log1p(float64(n)))})
	}
	slices.SortFunc(all, func(a, b kv) int {
		if a.w != b.w {
			if a.w > b.w {
				return -1
			}
			return 1
		}
		return int(a.id) - int(b.id)
	})
	out := map[string]float32{}
	for _, x := range all[:min(len(all), maxSparse)] {
		out[strconv.FormatUint(uint64(x.id), 10)] = x.w
	}
	return out
}

type tensor struct {
	Name     string   `json:"name"`
	Shape    []int    `json:"shape"`
	Datatype string   `json:"datatype"`
	Data     []string `json:"data"`
}

func main() {
	log := logging.New(os.Stdout, "mock-embedder", slog.LevelInfo)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/health/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /v2/models/"+model, func(w http.ResponseWriter, _ *http.Request) {
		// 输出列表与真实部署一致：调用方按有没有 sparse 输出判断能不能做稀疏召回
		_ = json.NewEncoder(w).Encode(map[string]any{"name": model, "versions": []string{version}, "platform": "mock",
			"outputs": []map[string]any{{"name": "embedding", "datatype": "FP32"}, {"name": "sparse", "datatype": "BYTES"}}})
	})
	mux.HandleFunc("POST /v2/models/"+model+"/infer", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Inputs []tensor `json:"inputs"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil || len(req.Inputs) != 1 || req.Inputs[0].Datatype != "BYTES" {
			http.Error(w, `{"error":"需要一个 BYTES 类型的输入"}`, http.StatusBadRequest)
			return
		}
		texts := req.Inputs[0].Data
		flat := make([]float32, 0, len(texts)*dim)
		sparses := make([]string, 0, len(texts))
		for _, t := range texts {
			flat = append(flat, embed(t)...)
			b, _ := json.Marshal(sparse(t))
			sparses = append(sparses, string(b))
		}
		// 第二个输出是稀疏向量：每段文本一个“词元 ID → 权重”的 JSON（与稠密向量同一次调用，retrieval 详设 6.2）
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model_name": model, "model_version": version,
			"outputs": []map[string]any{
				{"name": "embedding", "datatype": "FP32", "shape": []int{len(texts), dim}, "data": flat},
				{"name": "sparse", "datatype": "BYTES", "shape": []int{len(texts)}, "data": sparses},
			},
		})
	})
	// 模拟精排模型：分数 = 查询的字二元组有多少出现在段落里（与 mock-nli 同一算法），只够验证精排链路
	mux.HandleFunc("GET /v2/models/"+rerankModel, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"name": rerankModel, "versions": []string{version}, "platform": "mock"})
	})
	mux.HandleFunc("POST /v2/models/"+rerankModel+"/infer", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Inputs []tensor `json:"inputs"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		in := map[string][]string{}
		for _, t := range req.Inputs {
			in[t.Name] = t.Data
		}
		qs, ps := in["query"], in["passage"]
		if len(qs) == 0 || len(qs) != len(ps) {
			http.Error(w, `{"error":"需要等长的 query 与 passage"}`, http.StatusBadRequest)
			return
		}
		scores := make([]float64, len(qs))
		for i := range qs {
			// 映射到 0.5–1：模拟分数没有相关性含义，不应触发 orchestrator 的精排分下限（0.2），
			// 否则端到端测试里带样例标记的问题会因字面重合低而拿不到证据；下限的逻辑由单元测试覆盖
			scores[i] = 0.5 + 0.5*overlap(qs[i], ps[i])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model_name": rerankModel, "model_version": version,
			"outputs": []map[string]any{{"name": "score", "datatype": "FP32", "shape": []int{len(scores)}, "data": scores}},
		})
	})
	addr := os.Getenv("BF_LISTEN")
	if addr == "" {
		addr = ":8000"
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("mock-embedder 已启动")
	if err := srv.ListenAndServe(); err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}
