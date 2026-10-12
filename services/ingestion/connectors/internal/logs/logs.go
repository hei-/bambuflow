// Package logs 是日志数据源（技术设计文档 5.2、5.5）：订阅日志平台的消息主题，只入库配置的服务和级别，
// 先脱敏再进入管道，用 Drain 抽取模板后按 5 分钟时间窗口组织正文，权限按服务归属映射到运维组。
//
// 一份文档是“一个服务一天（UTC）”的日志，正文里每个 5 分钟窗口一节（processing 按标题分块，一个窗口一块）；
// 这一天就是保留期的时间分区，到期（默认 30 天）整份写删除标记。
// 增量标识是消息偏移量（消息 ID）：已处理过的消息 ID 记在这份文档的状态里，重复投递的消息不会重复计入，
// 正文没有变化时也不重发事件。消息在文档发出并保存状态之后才确认，中途失败的下一轮重读。
package logs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/hei-/bambuflow/libs/platform-go/logging"
)

// Window 是分块的时间窗口（技术设计文档 5.2）。
const Window = 5 * time.Minute

// Message 是日志平台主题里的一条消息。ID 是它在主题里的偏移量，单调递增。
type Message struct {
	ID      string
	Time    time.Time
	Service string
	Level   string
	Text    string
}

// Config 是日志数据源的部署配置：哪些服务、哪些级别入库，服务归哪些运维组。
type Config struct {
	Topic          string             `yaml:"topic"`
	RetentionDays  int                `yaml:"retention_days"`
	Classification string             `yaml:"classification"`
	Services       map[string]Service `yaml:"services"`
}

// Service 是一个服务的入库规则。
type Service struct {
	// 服务归属的运维组，展开成文档的权限 token（group:x）
	Groups []string `yaml:"groups"`
	Levels []string `yaml:"levels"`
}

var (
	servicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	groupPattern   = regexp.MustCompile(`^group:[^\s:]+$`)
	levels         = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	classes        = []string{"public", "internal", "confidential", "restricted"}
)

// LoadConfig 读取并校验配置；保留期不配置时取默认 30 天（技术设计文档 5.5），密级不配置时按内部。
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("日志数据源配置: %w", err)
	}
	if c.RetentionDays == 0 {
		c.RetentionDays = 30
	}
	if c.Classification == "" {
		c.Classification = "internal"
	}
	switch {
	case c.Topic == "":
		return nil, fmt.Errorf("日志数据源配置: topic 必填")
	case c.RetentionDays < 0:
		return nil, fmt.Errorf("日志数据源配置: retention_days 不能为负")
	case !slices.Contains(classes, c.Classification):
		return nil, fmt.Errorf("日志数据源配置: 密级 %q 不合法", c.Classification)
	case len(c.Services) == 0:
		return nil, fmt.Errorf("日志数据源配置: 至少配置一个服务")
	}
	for name, s := range c.Services {
		if !servicePattern.MatchString(name) {
			return nil, fmt.Errorf("服务名 %q 不合法", name)
		}
		// 没有归属的服务谁都看不到，等于白入库：直接拒绝配置，而不是放宽权限
		if len(s.Groups) == 0 {
			return nil, fmt.Errorf("服务 %s 没有配置归属的运维组", name)
		}
		for _, g := range s.Groups {
			if !groupPattern.MatchString(g) {
				return nil, fmt.Errorf("服务 %s 的运维组 %q 应为 group: 开头", name, g)
			}
		}
		for _, l := range s.Levels {
			if !slices.Contains(levels, l) {
				return nil, fmt.Errorf("服务 %s 的级别 %q 不合法", name, l)
			}
		}
	}
	return &c, nil
}

// accepts 判断一条消息是否入库：配置过的服务、配置过的级别、在保留期内。
func (c *Config) accepts(m Message, now time.Time) bool {
	s, ok := c.Services[m.Service]
	return ok && slices.Contains(s.Levels, strings.ToUpper(m.Level)) && !m.Time.Before(c.cutoff(now))
}

// cutoff 是保留期的起点（按 UTC 日对齐）：早于它的日志不入库，已入库的整天到期删除。
func (c *Config) cutoff(now time.Time) time.Time {
	day := now.UTC().Truncate(24 * time.Hour)
	return day.AddDate(0, 0, -c.RetentionDays)
}

// aclTokens 是服务的权限 token，排好序；aclFingerprint 与 docmeta 的规则相同。
func (c *Config) aclTokens(service string) []string {
	out := slices.Clone(c.Services[service].Groups)
	sort.Strings(out)
	return slices.Compact(out)
}

func (c *Config) aclFingerprint(service string) string {
	sum := sha256.Sum256([]byte(strings.Join(c.aclTokens(service), "\n") + "\n" + c.Classification))
	return hex.EncodeToString(sum[:])
}

// Entry 是文档状态里保存的一条消息（已脱敏）。
type Entry struct {
	ID    string    `json:"id"`
	Time  time.Time `json:"ts"`
	Level string    `json:"level"`
	Text  string    `json:"text"`
}

// DayState 是一份文档（一个服务一天）已经收到的消息，按偏移量排序。
type DayState struct {
	Entries []Entry `json:"entries"`
}

// add 加入新消息，返回实际新增的条数；偏移量已经出现过的（重复投递）不计入。
func (s *DayState) add(entries []Entry) int {
	seen := map[string]bool{}
	for _, e := range s.Entries {
		seen[e.ID] = true
	}
	n := 0
	for _, e := range entries {
		if !seen[e.ID] {
			seen[e.ID] = true
			s.Entries = append(s.Entries, e)
			n++
		}
	}
	sort.SliceStable(s.Entries, func(i, j int) bool { return offsetLess(s.Entries[i].ID, s.Entries[j].ID) })
	return n
}

// offsetLess 比较两个偏移量。Redis Streams 的 ID 是“毫秒-序号”，按数值比较；格式不同时按字符串比较。
func offsetLess(a, b string) bool {
	am, as, aok := splitOffset(a)
	bm, bs, bok := splitOffset(b)
	if !aok || !bok {
		return a < b
	}
	if am != bm {
		return am < bm
	}
	return as < bs
}

func splitOffset(id string) (uint64, uint64, bool) {
	var ms, seq uint64
	if _, err := fmt.Sscanf(id, "%d-%d", &ms, &seq); err != nil {
		return 0, 0, false
	}
	return ms, seq, true
}

// docID 是一个服务一天的文档 ID，如 payments-svc-20261005。
func docID(service string, day time.Time) string {
	return service + "-" + day.UTC().Format("20060102")
}

// dayOf 从文档 ID 取出日期；不是本数据源的格式时 ok 为 false。
func dayOf(id string) (time.Time, string, bool) {
	i := strings.LastIndex(id, "-")
	if i <= 0 {
		return time.Time{}, "", false
	}
	day, err := time.Parse("20060102", id[i+1:])
	if err != nil {
		return time.Time{}, "", false
	}
	return day, id[:i], true
}

// Render 把一天的消息写成正文：先按模板归并（Drain），再按 5 分钟窗口分节，每节列出窗口内出现过的模板、
// 级别、次数和首末时间。结果只取决于消息本身，同样的消息总是得到同样的正文。
func Render(service string, day time.Time, entries []Entry) string {
	d := NewDrain()
	type line struct {
		level       string
		tpl         *Template
		count       int
		first, last time.Time
	}
	windows := map[time.Time][]*line{}
	var order []time.Time
	for _, e := range entries {
		t := d.Add(e.Text)
		w := e.Time.UTC().Truncate(Window)
		if _, ok := windows[w]; !ok {
			order = append(order, w)
		}
		var l *line
		for _, x := range windows[w] {
			if x.tpl == t && x.level == e.Level {
				l = x
			}
		}
		if l == nil {
			l = &line{level: e.Level, tpl: t, first: e.Time}
			windows[w] = append(windows[w], l)
		}
		l.count++
		l.last = e.Time
	}
	sort.Slice(order, func(i, j int) bool { return order[i].Before(order[j]) })
	var b strings.Builder
	fmt.Fprintf(&b, "# %s 运行日志 %s（UTC）\n", service, day.UTC().Format(time.DateOnly))
	for _, w := range order {
		fmt.Fprintf(&b, "\n## %s–%s\n\n", w.Format("15:04"), w.Add(Window).Format("15:04"))
		for _, l := range windows[w] {
			// 模板在后面的消息里可能变得更泛：取最终的模板，正文仍然只由消息决定
			fmt.Fprintf(&b, "- [%s] %s（%d 次，%s–%s）\n", l.level, l.tpl, l.count,
				l.first.UTC().Format(time.TimeOnly), l.last.UTC().Format(time.TimeOnly))
		}
	}
	return b.String()
}

// Redact 在进入管道之前脱敏（技术设计文档 5.5“先脱敏再进入管道”）：与日志库的规则相同，
// 去掉令牌、身份证号、银行卡号、手机号和邮箱。
func Redact(text string) string { return logging.Redact(text) }

// accepted 把一批消息分到文档：不入库的（未配置的服务或级别、过了保留期）丢掉，其余先脱敏。
func (c *Config) accepted(msgs []Message, now time.Time) map[string][]Entry {
	out := map[string][]Entry{}
	for _, m := range msgs {
		if !c.accepts(m, now) {
			continue
		}
		id := docID(m.Service, m.Time.UTC().Truncate(24*time.Hour))
		out[id] = append(out[id], Entry{ID: m.ID, Time: m.Time.UTC(), Level: strings.ToUpper(m.Level), Text: Redact(m.Text)})
	}
	return out
}

// Subscriber 是日志平台消息主题的订阅端口（ADR-13）：开发与 CI 的默认实现是 Redis Streams 消费组（RedisTopic），
// 生产接公司日志平台的主题（多为 Kafka，只换实现）。Read 先返回本消费者已读未确认的消息，再返回新消息；
// 没有消息时返回空。
type Subscriber interface {
	Read(ctx context.Context, max int) ([]Message, error)
	Ack(ctx context.Context, ids []string) error
}
