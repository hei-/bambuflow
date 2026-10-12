// mock-llm 在没有公司 LLM 网关时顶替它，只用于开发与 CI（P0 计划第 7 步）。
//
// 实现 OpenAI 兼容的 /v1/chat/completions 子集（含 stream=true 的 SSE 输出），orchestrator 接真实网关时代码相同，只换地址。
// 输出按 orchestrator 的提示词格式（每行一个 JSON 单元）从证据区直接摘句，所以每句都有依据；
// 问题里带 [mock:<样例>] 标记时追加用于测试校验的输出（见 scenarios），真实问题不会带这种标记。
// 没有证据区的请求是 agent 路径的规划（orchestrator 详设第 6 节）：按 plan 的规则给出子问题，同样是确定的；
// 带 <groups> 的是主题汇总的合并（K2 详设第 6 节）：原样按组输出，标题与要点编号不变。
// 规划请求带 <views>（租户开放了 sql_query）且问题里有 [mock-sql:{参数}] 时，规划成一个 sql_query 子问题，参数原样给出。
//
// GET /mock/requests?contains=<文本> 返回问题里含该文本的请求数，端到端测试用它断言“没有调用 LLM”。
//
// 用量：响应带 usage（输入取全部消息的字数、输出取正文字数，确定的）；流式请求带 stream_options.include_usage
// 时在 [DONE] 之前单独发一个只有 usage 的数据块，与 OpenAI 兼容网关一致。
// 故障注入：POST /mock/faults {"fail_next": N} 让接下来 N 次 /v1/chat/completions 返回 503（不论问题内容），
// 用于测熔断；DELETE /mock/faults 清除。被注入故障的请求同样计入 /mock/requests。
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hei-/bambuflow/libs/platform-go/logging"
)

const (
	model = "mock-chat-1"
	// 每次最多摘几条证据成句
	maxLines = 4
)

var (
	// 与 orchestrator 的证据区格式一致：<evidence ref="S1" ...>正文</evidence>
	evidenceRe = regexp.MustCompile(`(?s)<evidence ref="S(\d+)"[^>]*>\n(.*?)\n</evidence>`)
	questionRe = regexp.MustCompile(`(?s)<question>\n(.*?)\n</question>`)
	scenarioRe = regexp.MustCompile(`\[mock:([a-z-]+)\]`)
	// 规划样例：[mock-plan:子问题1|子问题2] 指定拆出的子问题
	planRe    = regexp.MustCompile(`\[mock-plan:([^\]]+)\]`)
	markerRe  = regexp.MustCompile(`\[mock[^\]]*\]`)
	pendingRe = regexp.MustCompile(`(?s)<pending>\n(.*?)\n</pending>`)
	// 主题汇总合并的输入：<group id="g1" title="…">，其下每行“p3：要点”
	groupRe = regexp.MustCompile(`(?s)<group id="g\d+" title="([^"]*)">\n(.*?)\n</group>`)
	pointRe = regexp.MustCompile(`(?m)^(p\d+)：`)
)

// merge 模拟主题汇总的合并：每组原样输出为一行 {"title", "points"}。
func merge(prompt string) string {
	var lines []string
	for _, m := range groupRe.FindAllStringSubmatch(prompt, -1) {
		points := []string{}
		for _, p := range pointRe.FindAllStringSubmatch(m[2], -1) {
			points = append(points, p[1])
		}
		b, _ := json.Marshal(map[string]any{"title": m[1], "points": points})
		lines = append(lines, string(b))
	}
	return strings.Join(lines, "\n")
}

type subQuestion struct {
	ID   string          `json:"id"`
	Text string          `json:"text"`
	Tool string          `json:"tool,omitempty"`
	Args json.RawMessage `json:"args,omitempty"`
}

const sqlMarker = "[mock-sql:"

// sqlPlan 取出问题里 [mock-sql:{…}] 的参数（JSON 里可能有方括号，所以按 JSON 解析而不是用正则），返回去掉标记的问题。
func sqlPlan(q string) (json.RawMessage, string, bool) {
	i := strings.Index(q, sqlMarker)
	if i < 0 {
		return nil, q, false
	}
	dec := json.NewDecoder(strings.NewReader(q[i+len(sqlMarker):]))
	var args json.RawMessage
	if err := dec.Decode(&args); err != nil {
		return nil, q, false
	}
	end := i + len(sqlMarker) + int(dec.InputOffset())
	if end >= len(q) || q[end] != ']' {
		return nil, q, false
	}
	return args, strings.TrimSpace(q[:i] + q[end+1:]), true
}

// plan 模拟检索规划器。第 1 轮：问题带 [mock-plan:…] 时按它拆，否则把去掉标记的问题当作唯一的子问题；
// 第 2 轮起（有 <pending>）：每个未覆盖的子问题后面加“相关规定”作为新的检索表述。
// 问题带 [mock:plan-error] 或 [mock:error] 时返回 503。
func plan(prompt string) (string, int) {
	q := ""
	if m := questionRe.FindStringSubmatch(prompt); m != nil {
		q = m[1]
	}
	if strings.Contains(q, "[mock:plan-error]") || strings.Contains(q, "[mock:error]") {
		return "", http.StatusServiceUnavailable
	}
	var subs []subQuestion
	if m := pendingRe.FindStringSubmatch(prompt); m != nil {
		for _, l := range strings.Split(m[1], "\n") {
			id, text, ok := strings.Cut(l, "：")
			if ok {
				subs = append(subs, subQuestion{ID: id, Text: text + " 相关规定"})
			}
		}
	} else if args, text, ok := sqlPlan(q); ok && strings.Contains(prompt, "<views>") {
		subs = []subQuestion{{ID: "q1", Text: strings.TrimSpace(markerRe.ReplaceAllString(text, "")), Tool: "sql_query", Args: args}}
	} else if m := planRe.FindStringSubmatch(q); m != nil {
		for i, text := range strings.Split(m[1], "|") {
			subs = append(subs, subQuestion{ID: fmt.Sprintf("q%d", i+1), Text: strings.TrimSpace(text)})
		}
	} else {
		subs = []subQuestion{{ID: "q1", Text: strings.TrimSpace(markerRe.ReplaceAllString(q, ""))}}
	}
	var lines []string
	for _, s := range subs {
		b, _ := json.Marshal(s)
		lines = append(lines, string(b))
	}
	return strings.Join(lines, "\n"), http.StatusOK
}

type unit struct {
	BlockID   string   `json:"block_id"`
	BlockType string   `json:"block_type"`
	Text      string   `json:"text"`
	Refs      []int    `json:"refs"`
	Quote     []string `json:"quote"`
	ClaimType string   `json:"claim_type"`
}

// firstSentence 取证据正文里第一句够长的非标题句子（含句末标点）。
func firstSentence(text string) string {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, s := range splitSentences(line) {
			if utf8.RuneCountInString(s) >= 8 {
				return truncate(s, 200)
			}
		}
	}
	return ""
}

func splitSentences(line string) []string {
	var out []string
	start := 0
	for i, r := range line {
		if strings.ContainsRune("。！？；", r) {
			end := i + utf8.RuneLen(r)
			out = append(out, line[start:end])
			start = end
		}
	}
	if start < len(line) {
		out = append(out, line[start:])
	}
	return out
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// answer 按证据出现顺序摘句生成回答正文，再按问题里的样例标记追加输出；返回正文和 HTTP 状态码。
func answer(prompt string) (string, int) {
	var units []unit
	var first string
	firstRef := 0
	for _, m := range evidenceRe.FindAllStringSubmatch(prompt, -1) {
		if len(units) == maxLines {
			break
		}
		s := firstSentence(m[2])
		if s == "" {
			continue
		}
		var ref int
		_, _ = fmt.Sscanf(m[1], "%d", &ref)
		if first == "" {
			first, firstRef = s, ref
		}
		units = append(units, unit{
			BlockID: fmt.Sprintf("b%d", len(units)+1), BlockType: "paragraph",
			Text: s + fmt.Sprintf("[S%d]", ref), Refs: []int{ref}, Quote: []string{s}, ClaimType: "quote",
		})
	}
	var lines []string
	for _, u := range units {
		b, _ := json.Marshal(u)
		lines = append(lines, string(b))
	}
	q := ""
	if m := questionRe.FindStringSubmatch(prompt); m != nil {
		q = m[1]
	}
	status := http.StatusOK
	for _, sc := range scenarioRe.FindAllStringSubmatch(q, -1) {
		extra, st := scenario(sc[1], first, firstRef)
		if st != 0 {
			status = st
		}
		lines = append(lines, extra...)
	}
	return strings.Join(lines, "\n"), status
}

// scenarios 是测试样例：参数是第一条证据的摘句和编号，返回追加的输出行；第二个返回值非 0 时用作 HTTP 状态码。
func scenarios() map[string]func(quote string, ref int) ([]string, int) {
	line := func(u unit) string { b, _ := json.Marshal(u); return string(b) }
	return map[string]func(string, int) ([]string, int){
		// 引文是真的，但句子与引文无关：NLI 判定不支持
		"ungrounded": func(quote string, ref int) ([]string, int) {
			return []string{line(unit{BlockID: "bx", BlockType: "paragraph", Text: fmt.Sprintf("所有客户均可享受三年免息优惠。[S%d]", ref),
				Refs: []int{ref}, Quote: []string{quote}, ClaimType: "quote"})}, 0
		},
		// 引文不在所引证据里：引文核对不通过
		"fake-quote": func(_ string, ref int) ([]string, int) {
			return []string{line(unit{BlockID: "bx", BlockType: "paragraph", Text: fmt.Sprintf("贷款期限最长为三十年。[S%d]", ref),
				Refs: []int{ref}, Quote: []string{"贷款期限最长为三十年。"}, ClaimType: "quote"})}, 0
		},
		// 引用了不存在的证据编号
		"bad-ref": func(quote string, _ int) ([]string, int) {
			return []string{line(unit{BlockID: "bx", BlockType: "paragraph", Text: quote + "[S99]",
				Refs: []int{99}, Quote: []string{quote}, ClaimType: "quote"})}, 0
		},
		// 没有引用的句子
		"no-refs": func(string, int) ([]string, int) {
			return []string{line(unit{BlockID: "bx", BlockType: "paragraph", Text: "以上内容仅供参考。", ClaimType: "none"})}, 0
		},
		// 网关不可用
		"error": func(string, int) ([]string, int) { return nil, http.StatusServiceUnavailable },
		// 输出不是 JSON
		"garbage": func(string, int) ([]string, int) { return []string{"这不是 JSON", "也不是"}, 0 },
	}
}

func scenario(name, quote string, ref int) ([]string, int) {
	f, ok := scenarios()[name]
	if !ok {
		return nil, 0
	}
	return f(quote, ref)
}

// streamPiece 是流式输出时每个数据块的字数。取得比一行短，让行跨越多个数据块，与真实网关一样需要调用方拼行。
const streamPiece = 7

// chunks 把正文按 streamPiece 个字切成数据块。
func chunks(content string) []string {
	r := []rune(content)
	var out []string
	for i := 0; i < len(r); i += streamPiece {
		out = append(out, string(r[i:min(i+streamPiece, len(r))]))
	}
	return out
}

// writeStream 按 OpenAI 兼容的 stream=true 格式（SSE，data: 后接 chat.completion.chunk，最后 data: [DONE]）输出正文；
// usage 不为 nil 时在 [DONE] 之前单独发一个 choices 为空、只带 usage 的数据块。
func writeStream(w http.ResponseWriter, content string, usage map[string]int) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	send := func(delta map[string]string, finish any) {
		b, _ := json.Marshal(map[string]any{
			"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
		_ = rc.Flush()
	}
	send(map[string]string{"role": "assistant"}, nil)
	for _, c := range chunks(content) {
		send(map[string]string{"content": c}, nil)
	}
	send(map[string]string{}, "stop")
	if usage != nil {
		b, _ := json.Marshal(map[string]any{
			"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
			"choices": []any{}, "usage": usage,
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	_ = rc.Flush()
}

type recorder struct {
	mu        sync.Mutex
	questions []string
}

func (r *recorder) add(q string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// 只保留最近 1000 条，长期运行也不会占满内存
	if len(r.questions) == 1000 {
		r.questions = r.questions[1:]
	}
	r.questions = append(r.questions, q)
}

func (r *recorder) count(sub string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, q := range r.questions {
		if strings.Contains(q, sub) {
			n++
		}
	}
	return n
}

// faults 是注入的故障：接下来 failNext 次补全请求返回 503。
type faults struct {
	mu       sync.Mutex
	failNext int
}

func (f *faults) set(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext = n
}

// take 消耗一次故障，返回这次请求是否应失败。
func (f *faults) take() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext <= 0 {
		return false
	}
	f.failNext--
	return true
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// usageOf 是确定的用量：输入为全部消息的字数，输出为正文的字数。
func usageOf(messages []message, content string) map[string]int {
	in := 0
	for _, m := range messages {
		in += utf8.RuneCountInString(m.Content)
	}
	out := utf8.RuneCountInString(content)
	return map[string]int{"prompt_tokens": in, "completion_tokens": out, "total_tokens": in + out}
}

func newMux(rec *recorder, flt *faults) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": []map[string]any{{"id": model, "object": "model"}}})
	})
	mux.HandleFunc("GET /mock/requests", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int{"count": rec.count(r.URL.Query().Get("contains"))})
	})
	mux.HandleFunc("POST /mock/faults", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			FailNext int `json:"fail_next"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&req); err != nil || req.FailNext < 0 {
			http.Error(w, `{"error":{"message":"invalid request"}}`, http.StatusBadRequest)
			return
		}
		flt.set(req.FailNext)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /mock/faults", func(w http.ResponseWriter, _ *http.Request) {
		flt.set(0)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model         string    `json:"model"`
			Stream        bool      `json:"stream"`
			Messages      []message `json:"messages"`
			StreamOptions struct {
				IncludeUsage bool `json:"include_usage"`
			} `json:"stream_options"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || len(req.Messages) == 0 {
			http.Error(w, `{"error":{"message":"invalid request"}}`, http.StatusBadRequest)
			return
		}
		if req.Model != model {
			http.Error(w, `{"error":{"message":"model not found"}}`, http.StatusNotFound)
			return
		}
		prompt := req.Messages[len(req.Messages)-1].Content
		if m := questionRe.FindStringSubmatch(prompt); m != nil {
			rec.add(m[1])
		}
		if flt.take() {
			http.Error(w, `{"error":{"message":"mock fault injected"}}`, http.StatusServiceUnavailable)
			return
		}
		content, status := answer(prompt)
		switch {
		case strings.Contains(prompt, "<groups>"):
			content, status = merge(prompt), http.StatusOK
		case !strings.Contains(prompt, "<evidence"):
			content, status = plan(prompt)
		}
		if status != http.StatusOK {
			http.Error(w, `{"error":{"message":"mock unavailable"}}`, status)
			return
		}
		usage := usageOf(req.Messages, content)
		if req.Stream {
			if !req.StreamOptions.IncludeUsage {
				usage = nil
			}
			writeStream(w, content, usage)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-mock", "object": "chat.completion", "created": time.Now().Unix(), "model": model,
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": content}, "finish_reason": "stop"}},
			"usage":   usage,
		})
	})
	return mux
}

func main() {
	log := logging.New(os.Stdout, "mock-llm", slog.LevelInfo)
	mux := newMux(&recorder{}, &faults{})
	addr := os.Getenv("BF_LISTEN")
	if addr == "" {
		addr = ":8000"
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("mock-llm 已启动")
	if err := srv.ListenAndServe(); err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}
