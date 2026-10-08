package logs

import (
	"regexp"
	"strings"
)

// Drain 是 Drain 模板抽取（He et al., ICWS 2017）的最小实现：按词数和首词分桶，桶内按逐位相同的比例
// 找最相近的模板，够相近就合并（不同的位置换成 <*>），否则新建模板。没有引入现成的库：
// 需要的只有这几十行，结果只取决于输入顺序，同一批消息总是得到同样的模板（重复入库不产生变化）。
//
// 预处理先把明显的变量（数字、十六进制串、UUID、已脱敏的占位符）换成 <*>，再按空白切词。
type Drain struct {
	// Similarity 是合并的门槛：逐位相同的词占比达到它才算同一模板（原论文建议 0.4–0.5）
	Similarity float64
	buckets    map[string][]*Template
	all        []*Template
}

// Template 是一个日志模板；Tokens 里的 <*> 是变量位置。
type Template struct {
	ID     int
	Tokens []string
}

func (t *Template) String() string { return strings.Join(t.Tokens, " ") }

const wildcard = "<*>"

var variables = regexp.MustCompile(`\[已脱敏:[a-z_]+\]|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|\b0x[0-9a-fA-F]+\b|[A-Za-z]*\d[\w.:-]*`)

// NewDrain 返回空的抽取器。
func NewDrain() *Drain {
	return &Drain{Similarity: 0.5, buckets: map[string][]*Template{}}
}

// Add 把一条消息归到某个模板并返回它。模板可能因这条消息而变得更泛（多一个 <*>）。
func (d *Drain) Add(message string) *Template {
	tokens := strings.Fields(variables.ReplaceAllString(message, wildcard))
	if len(tokens) == 0 {
		tokens = []string{wildcard}
	}
	first := tokens[0]
	key := strings.Repeat("#", len(tokens)) + " " + first
	var best *Template
	bestSim := -1.0
	for _, t := range d.buckets[key] {
		if s := similarity(t.Tokens, tokens); s > bestSim {
			best, bestSim = t, s
		}
	}
	if best != nil && bestSim >= d.Similarity {
		for i, tok := range tokens {
			if best.Tokens[i] != tok {
				best.Tokens[i] = wildcard
			}
		}
		return best
	}
	t := &Template{ID: len(d.all) + 1, Tokens: tokens}
	d.buckets[key] = append(d.buckets[key], t)
	d.all = append(d.all, t)
	return t
}

func similarity(a, b []string) float64 {
	same := 0
	for i := range a {
		if a[i] == b[i] {
			same++
		}
	}
	return float64(same) / float64(len(a))
}
