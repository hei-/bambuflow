// loadtest 是压测工具（技术设计文档 14.4 的压测计划，docs/loadtest-plan.md）：按给定的到达速率对 gateway 的 /v1/answers
// 发请求（开环：到点就发，不等前一个返回，最多同时 -max-inflight 个），按分钟统计耗时分位、错误率与回答状态，
// 结束时按通过标准判定，报告写到 -out（report.json、report.md）。
//
//	loadtest -base https://rag.example -tokens tokens.txt -questions questions.jsonl -rate 50 -duration 2h -out out/
//	loadtest ... -rate 10 -to 100 -ramp 5m -duration 30m          # 峰值突增：5 分钟内从 10 拉到 100，之后保持
//	loadtest ... -batch 10000                                     # 批量叠加：开始时提交一个 1 万条的批次，结束时报告它的进度
//
// tokens.txt 每行一个用户的 Bearer 令牌（不同用户轮流用，令牌不进报告）；questions.jsonl 每行
// {"question": "...", "kind": "single_hop" | "multi_hop"}，按 -multihop 的比例抽多跳题，其余抽单跳题。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	var c config
	flag.StringVar(&c.base, "base", "", "gateway 地址")
	flag.StringVar(&c.tenant, "tenant", "", "平台级客户端用的 Bambuflow-Tenant 请求头；业务系统客户端绑定了租户时不填")
	tokens := flag.String("tokens", "", "令牌文件，每行一个")
	questions := flag.String("questions", "", "问题文件（JSONL）")
	flag.Float64Var(&c.rate, "rate", 50, "每秒请求数（-to 给出时是起点）")
	flag.Float64Var(&c.to, "to", 0, "爬升到的每秒请求数；0 表示不爬升")
	flag.DurationVar(&c.ramp, "ramp", 0, "从 -rate 爬升到 -to 用的时间")
	flag.DurationVar(&c.duration, "duration", 10*time.Minute, "总时长")
	flag.Float64Var(&c.multihop, "multihop", 0, "多跳题的比例（0–1）")
	flag.BoolVar(&c.stream, "stream", true, "用 SSE 按句返回，同时测第一句到达时间")
	flag.IntVar(&c.maxInflight, "max-inflight", 2000, "同时在途的请求上限，超过的计为 dropped（压测端自身的保护）")
	flag.DurationVar(&c.timeout, "timeout", 60*time.Second, "单个请求的超时")
	flag.IntVar(&c.batch, "batch", 0, "开始时提交一个这么多条的批次（批量叠加场景），0 为不提交")
	out := flag.String("out", "loadtest-out", "报告目录")
	flag.DurationVar(&c.criteria.p95, "pass-p95", 4*time.Second, "通过标准：单跳 p95")
	flag.DurationVar(&c.criteria.multihopP95, "pass-multihop-p95", 10*time.Second, "通过标准：多跳 p95")
	flag.Float64Var(&c.criteria.errorRate, "pass-error-rate", 0.001, "通过标准：错误率（5xx、超时与连接失败）上限")
	flag.Parse()
	if c.base == "" || *tokens == "" || *questions == "" {
		flag.Usage()
		os.Exit(2)
	}
	var err error
	if c.tokens, err = readTokens(*tokens); err != nil {
		log.Fatal(err)
	}
	if c.questions, err = readQuestions(*questions); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c.client = &http.Client{Timeout: c.timeout}
	rep, err := run(ctx, &c)
	if err != nil {
		log.Fatal(err)
	}
	if err := write(*out, rep); err != nil {
		log.Fatal(err)
	}
	fmt.Print(rep.markdown())
	if !rep.Passed {
		os.Exit(1)
	}
}

func readTokens(path string) ([]string, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if t := strings.TrimSpace(sc.Text()); t != "" && !strings.HasPrefix(t, "#") {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("令牌文件是空的")
	}
	return out, sc.Err()
}

type question struct {
	Question string `json:"question"`
	Kind     string `json:"kind"`
}

func readQuestions(path string) (map[string][]string, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	out := map[string][]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var q question
		if err := json.Unmarshal([]byte(line), &q); err != nil || q.Question == "" {
			return nil, fmt.Errorf("问题文件第 %d 行不合法", n)
		}
		switch q.Kind {
		case "", kindSingle:
			out[kindSingle] = append(out[kindSingle], q.Question)
		case kindMulti:
			out[kindMulti] = append(out[kindMulti], q.Question)
		default:
			return nil, fmt.Errorf("问题文件第 %d 行的 kind 只能是 single_hop 或 multi_hop", n)
		}
	}
	if len(out[kindSingle]) == 0 {
		return nil, errors.New("问题文件里没有单跳题")
	}
	return out, sc.Err()
}

func write(dir string, rep *report) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), b, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.md"), []byte(rep.markdown()), 0o600)
}
