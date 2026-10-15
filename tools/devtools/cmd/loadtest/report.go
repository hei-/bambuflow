package main

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// stats 是一组请求的统计。耗时只算拿到完整回答（200 且收到 final）的请求；错误是 5xx、超时、连接失败与没收到 final 就断开，
// 429 单独计（限流与配额是预期内的保护，不算错误）。
type stats struct {
	Requests int            `json:"requests"`
	Errors   int            `json:"errors"`
	Limited  int            `json:"limited"`
	Dropped  int            `json:"dropped"`
	P50      float64        `json:"p50_ms"`
	P95      float64        `json:"p95_ms"`
	P99      float64        `json:"p99_ms"`
	FirstP95 float64        `json:"first_sentence_p95_ms"`
	Statuses map[string]int `json:"statuses"`
	Degraded int            `json:"degraded"`
}

func (s stats) errorRate() float64 {
	if s.Requests == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Requests)
}

type window struct {
	Minute int              `json:"minute"`
	Rate   float64          `json:"rate"`
	Kinds  map[string]stats `json:"kinds"`
}

type check struct {
	Name string `json:"name"`
	Want string `json:"want"`
	Got  string `json:"got"`
	Pass bool   `json:"pass"`
}

type report struct {
	Duration string           `json:"duration"`
	Rate     float64          `json:"rate"`
	To       float64          `json:"to,omitempty"`
	Ramp     string           `json:"ramp,omitempty"`
	Multihop float64          `json:"multihop"`
	Stream   bool             `json:"stream"`
	Overall  map[string]stats `json:"overall"`
	Windows  []window         `json:"windows"`
	Checks   []check          `json:"checks"`
	Passed   bool             `json:"passed"`
	Batch    *batchProgress   `json:"batch,omitempty"`
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := max(int(math.Ceil(p*float64(len(sorted))))-1, 0)
	return float64(sorted[i]) / float64(time.Millisecond)
}

func collect(samples []sample) stats {
	s := stats{Statuses: map[string]int{}}
	var lat, first []time.Duration
	for _, x := range samples {
		s.Requests++
		switch {
		case x.Dropped:
			s.Dropped++
		case x.Code == 0 || x.Code >= 500:
			s.Errors++
		case x.Code == 429:
			s.Limited++
		case x.Code == 200 && x.Problem != "":
			// 流式的 error 事件：deadline_exceeded、service_unavailable 算错误
			s.Errors++
		case x.Code == 200:
			lat = append(lat, x.Latency)
			if x.First > 0 {
				first = append(first, x.First)
			}
			s.Statuses[x.Status]++
			if x.Degraded {
				s.Degraded++
			}
		}
	}
	slices.Sort(lat)
	slices.Sort(first)
	s.P50, s.P95, s.P99 = percentile(lat, 0.5), percentile(lat, 0.95), percentile(lat, 0.99)
	s.FirstP95 = percentile(first, 0.95)
	return s
}

func byKind(samples []sample) map[string]stats {
	groups := map[string][]sample{}
	for _, s := range samples {
		groups[s.Kind] = append(groups[s.Kind], s)
	}
	out := map[string]stats{}
	for k, g := range groups {
		out[k] = collect(g)
	}
	return out
}

func summarize(samples []sample, c *config, elapsed time.Duration) *report {
	r := &report{Duration: elapsed.Round(time.Second).String(), Rate: c.rate, To: c.to, Multihop: c.multihop, Stream: c.stream,
		Overall: byKind(samples)}
	if c.ramp > 0 {
		r.Ramp = c.ramp.String()
	}
	minutes := map[int][]sample{}
	for _, s := range samples {
		m := int(s.At / time.Minute)
		minutes[m] = append(minutes[m], s)
	}
	for m := 0; m <= int(elapsed/time.Minute); m++ {
		if len(minutes[m]) == 0 {
			continue
		}
		r.Windows = append(r.Windows, window{Minute: m, Rate: float64(len(minutes[m])) / 60, Kinds: byKind(minutes[m])})
	}
	single, multi := r.Overall[kindSingle], r.Overall[kindMulti]
	all := collect(samples)
	r.Checks = append(r.Checks,
		check{"单跳 p95", "≤ " + c.criteria.p95.String(), ms(single.P95), single.P95 <= ms64(c.criteria.p95)},
		check{"错误率", fmt.Sprintf("< %.2f%%", c.criteria.errorRate*100), fmt.Sprintf("%.3f%%", all.errorRate()*100),
			all.errorRate() <= c.criteria.errorRate},
		// 压测端自己丢弃了请求说明负载没有按计划发出去，结果不能用
		check{"压测端没有丢弃请求", "0", fmt.Sprint(all.Dropped), all.Dropped == 0},
	)
	if multi.Requests > 0 {
		r.Checks = append(r.Checks, check{"多跳 p95", "≤ " + c.criteria.multihopP95.String(), ms(multi.P95),
			multi.P95 <= ms64(c.criteria.multihopP95)})
	}
	r.Passed = !slices.ContainsFunc(r.Checks, func(x check) bool { return !x.Pass })
	return r
}

func ms64(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func ms(v float64) string { return fmt.Sprintf("%.0f ms", v) }

func (r *report) markdown() string {
	var b strings.Builder
	verdict := "通过"
	if !r.Passed {
		verdict = "不通过"
	}
	fmt.Fprintf(&b, "# 压测报告：%s\n\n时长 %s，速率 %.0f/s", verdict, r.Duration, r.Rate)
	if r.To > 0 {
		fmt.Fprintf(&b, " → %.0f/s（%s 内爬升）", r.To, r.Ramp)
	}
	fmt.Fprintf(&b, "，多跳比例 %.0f%%，流式 %v\n\n", r.Multihop*100, r.Stream)
	b.WriteString("| 检查 | 标准 | 实际 | 结果 |\n| --- | --- | --- | --- |\n")
	for _, c := range r.Checks {
		mark := "通过"
		if !c.Pass {
			mark = "**不通过**"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", c.Name, c.Want, c.Got, mark)
	}
	b.WriteString("\n| 题型 | 请求 | 错误 | 429 | p50 | p95 | p99 | 第一句 p95 | 降级 | 回答状态 |\n" +
		"| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, k := range []string{kindSingle, kindMulti} {
		if s, ok := r.Overall[k]; ok {
			fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %s | %s | %s | %d | %s |\n", k, s.Requests, s.Errors, s.Limited,
				ms(s.P50), ms(s.P95), ms(s.P99), ms(s.FirstP95), s.Degraded, statuses(s.Statuses))
		}
	}
	b.WriteString("\n按分钟（单跳）：\n\n| 分钟 | 实际速率 | 错误 | 429 | p95 |\n| --- | --- | --- | --- | --- |\n")
	for _, w := range r.Windows {
		s := w.Kinds[kindSingle]
		fmt.Fprintf(&b, "| %d | %.1f/s | %d | %d | %s |\n", w.Minute, w.Rate, s.Errors, s.Limited, ms(s.P95))
	}
	if r.Batch != nil {
		fmt.Fprintf(&b, "\n批次 %s：%s，%v %s\n", r.Batch.BatchID, r.Batch.Status, r.Batch.Counts, r.Batch.Error)
	}
	return b.String()
}

func statuses(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, m[k])
	}
	return strings.Join(parts, "，")
}
