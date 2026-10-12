// Package audit 写入口审计（gateway 详设 8.1）：每个请求一条，包括在认证、授权阶段被拒绝的。
//
// 以 JSON 行追加写入本地文件并逐条落盘（技术设计文档 8.3 的本地持久队列），Shipper 定期切段送往开启对象锁的桶。
// 写入失败时返回错误，调用方据此拒绝请求：审计不可用即 fail-closed。只记问题的摘要和长度，不记原文。
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Record struct {
	Time     time.Time `json:"time"`
	TraceID  string    `json:"trace_id"`
	AnswerID string    `json:"answer_id,omitempty"`
	// 批量接口与批量换发所涉及的批次
	BatchID  string `json:"batch_id,omitempty"`
	JTI      string `json:"jti,omitempty"`
	Sub      string `json:"sub,omitempty"`
	Tenant   string `json:"tenant,omitempty"`
	Actor    string `json:"actor,omitempty"`
	Endpoint string `json:"endpoint"`
	Status   int    `json:"status"`
	Reason   string `json:"reason,omitempty"`
	// 只有 SSE 请求填写：open 在开始输出前写（写不进去就不输出），end 在流结束时写，
	// end 的 reason 为空表示正常结束，cancelled 表示客户端中途断开，其余为 error 事件的错误类型
	Stream      string `json:"stream,omitempty"`
	LatencyMS   int64  `json:"latency_ms"`
	QuestionSHA string `json:"question_sha256,omitempty"`
	QuestionLen int    `json:"question_len,omitempty"`
	// 问答的三类块（技术设计文档 8.3）：检索候选、送入模型的上下文、展示给用户的引用
	CandidateChunkIDs []string `json:"candidate_chunk_ids,omitempty"`
	ContextChunkIDs   []string `json:"context_chunk_ids,omitempty"`
	ShownChunkIDs     []string `json:"shown_chunk_ids,omitempty"`
	// 枚举、计数类回答列出的文档与统计到的总数（K2 详设第 8 节）
	ListedDocIDs []string `json:"listed_doc_ids,omitempty"`
	TotalCount   int64    `json:"total_count,omitempty"`
	// orchestrator 回报的实际 token 用量（计入租户配额，不对外返回）
	PromptTokens     int64 `json:"prompt_tokens,omitempty"`
	CompletionTokens int64 `json:"completion_tokens,omitempty"`
	// 影响结果的版本（技术设计文档 8.3）：问答取自 why，仅检索取自检索响应。回答记录按保留期销毁原文后，
	// 审计里仍能查到当时用的是哪个索引、模型、提示词与策略
	Versions *Versions `json:"versions,omitempty"`
	// 问答的结果状态（answered、insufficient_evidence、refused 等），拒答与降级的原因见 Versions.Degraded；
	// 回答摘要是已推送句子的文本按顺序连接后的 SHA-256，审计不存回答原文
	AnswerStatus string `json:"answer_status,omitempty"`
	AnswerSHA    string `json:"answer_sha256,omitempty"`
	// 回放时调用者凭什么看到这条回答：asker（提问人本人）或 auditor（审计岗）
	Access string `json:"access,omitempty"`
	// restricted_access：受限级内容的单独访问记录（技术设计文档 8.6“每次访问单独审计”），在内容发出之前写，
	// 写不进去就不发；ShownChunkIDs 与 ListedDocIDs 只含这一次发出的受限级块与文档。普通的入口审计为空
	Event string `json:"event,omitempty"`
	// 管理接口才有（gateway 详设 8.2）
	Admin *Admin `json:"admin,omitempty"`
}

// Versions 是一次问答或检索用到的版本组合与降级。
type Versions struct {
	IndexVersion      string `json:"index_version,omitempty"`
	PolicyVersion     string `json:"policy_version,omitempty"`
	EmbeddingModel    string `json:"embedding_model,omitempty"`
	RerankModel       string `json:"rerank_model,omitempty"`
	GenerationModel   string `json:"generation_model,omitempty"`
	PromptVersion     string `json:"prompt_version,omitempty"`
	NLIModel          string `json:"nli_model,omitempty"`
	CalculatorVersion string `json:"calculator_version,omitempty"`
	ExperimentID      string `json:"experiment_id,omitempty"`
	ExperimentArm     string `json:"experiment_arm,omitempty"`
	PipelineVersion   string `json:"pipeline_version,omitempty"`
	RoutingVersion    string `json:"routing_version,omitempty"`
	// 降级与拒答原因：阶段:原因
	Degraded []string `json:"degraded,omitempty"`
}

// AnswerDigest 是已推送句子的摘要；没有句子时为空。
func AnswerDigest(sentences []string) string {
	if len(sentences) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join(sentences, "\n")))
	return hex.EncodeToString(sum[:])
}

// Admin 是管理操作审计：谁以什么角色、对哪个实现方做了什么。写操作另记请求体摘要与实现方返回的结果，
// 实现方的业务审计记同一个 trace_id，两边据此关联。
type Admin struct {
	Owner  string   `json:"owner"`
	Method string   `json:"method"`
	Path   string   `json:"path"`
	Roles  []string `json:"roles,omitempty"`
	// 只有写操作填写：请求体的 SHA-256、脱敏后的请求体与结果（各最多 MaxSummary 字节）
	RequestSHA     string `json:"request_sha256,omitempty"`
	Request        string `json:"request,omitempty"`
	Result         string `json:"result,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// 同一幂等键的重复请求：没有再调用实现方，返回的是第一次的结果
	Replayed bool `json:"replayed,omitempty"`
}

// EventRestricted 是受限级内容的单独访问记录。
const EventRestricted = "restricted_access"

// MaxSummary 是管理审计里请求体与结果摘要的长度上限（字节）。超出部分截断，完整内容的摘要由 RequestSHA 保留。
const MaxSummary = 2048

// sensitive 匹配要脱敏的字段名：密钥、令牌、口令一类的值不进审计。
var sensitive = regexp.MustCompile(`(?i)secret|token|password|passwd|credential|private|api_?key|authorization`)

// Summarize 返回 JSON 的脱敏摘要：字段名像密钥、令牌、口令的值替换为 "***"，再截断到 MaxSummary 字节。
// 不是合法 JSON 时不记内容，只记长度，避免把无法识别结构的原文写进审计。
func Summarize(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return fmt.Sprintf("<非 JSON，%d 字节>", len(body))
	}
	out, err := json.Marshal(redact(v))
	if err != nil {
		return fmt.Sprintf("<无法摘要，%d 字节>", len(body))
	}
	if len(out) > MaxSummary {
		// 按 UTF-8 字符边界截断
		cut := MaxSummary
		for cut > 0 && !utf8.RuneStart(out[cut]) {
			cut--
		}
		return string(out[:cut]) + "…"
	}
	return string(out)
}

func redact(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			if sensitive.MatchString(k) {
				x[k] = "***"
			} else {
				x[k] = redact(val)
			}
		}
	case []any:
		for i := range x {
			x[i] = redact(x[i])
		}
	}
	return v
}

// QuestionDigest 返回问题原文的摘要与字数。
func QuestionDigest(q string) (string, int) {
	if q == "" {
		return "", 0
	}
	sum := sha256.Sum256([]byte(q))
	return hex.EncodeToString(sum[:]), utf8.RuneCountInString(q)
}

type Writer struct {
	mu   sync.Mutex
	path string
	f    *os.File
	n    int64 // 当前段已写字节数
}

func Open(path string) (*Writer, error) {
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	w := &Writer{path: path}
	return w, w.open()
}

func (w *Writer) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.f, w.n = f, st.Size()
	return nil
}

// Rotate 把当前段封存为 <path>.<纳秒时间戳> 并开新段；当前段为空时不动。返回是否封存了一段。
func (w *Writer) Rotate(now time.Time) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.n == 0 {
		return false, nil
	}
	if err := w.f.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(w.path, fmt.Sprintf("%s.%019d", w.path, now.UnixNano())); err != nil {
		// 改名失败就继续写原文件，不丢记录
		return false, errors.Join(err, w.open())
	}
	return true, w.open()
}

// Sealed 按时间顺序返回已封存、尚未送出的段。
func (w *Writer) Sealed() ([]string, error) {
	out, err := filepath.Glob(w.path + ".*")
	sort.Strings(out)
	return out, err
}

// Write 追加一条并落盘；失败返回错误。
func (w *Writer) Write(r Record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.f.Write(append(line, '\n'))
	w.n += int64(n)
	if err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error { return w.f.Close() }
