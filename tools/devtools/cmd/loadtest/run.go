package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	kindSingle = "single_hop"
	kindMulti  = "multi_hop"
)

type criteria struct {
	p95, multihopP95 time.Duration
	errorRate        float64
}

type config struct {
	base, tenant   string
	tokens         []string
	questions      map[string][]string
	rate, to       float64
	ramp, duration time.Duration
	multihop       float64
	stream         bool
	maxInflight    int
	timeout        time.Duration
	batch          int
	criteria       criteria
	client         *http.Client
}

// rateAt 是开始后 t 时刻的目标速率：-to 给出时在 -ramp 内线性爬升，之后保持
func (c *config) rateAt(t time.Duration) float64 {
	if c.to <= 0 || c.ramp <= 0 {
		return c.rate
	}
	if t >= c.ramp {
		return c.to
	}
	return c.rate + (c.to-c.rate)*float64(t)/float64(c.ramp)
}

// mixer 按比例轮流出题：多跳题的比例用累加的方式摊匀（每累计满 1 出一道多跳题），各类题按顺序循环，
// 同样的参数每次压测发出的题目序列相同，结果可以对比
type mixer struct {
	questions map[string][]string
	share     float64
	acc       float64
	pos       map[string]int
}

func newMixer(questions map[string][]string, share float64) *mixer {
	if len(questions[kindMulti]) == 0 {
		share = 0
	}
	return &mixer{questions: questions, share: share, pos: map[string]int{}}
}

func (m *mixer) next() (string, string) {
	kind := kindSingle
	if m.acc += m.share; m.acc >= 1 {
		m.acc--
		kind = kindMulti
	}
	qs := m.questions[kind]
	q := qs[m.pos[kind]%len(qs)]
	m.pos[kind]++
	return kind, q
}

// sample 是一个请求的结果
type sample struct {
	At      time.Duration // 发出时刻（相对开始）
	Kind    string
	Latency time.Duration
	// First 是第一句到达的时间（流式）；没有句子时为 0
	First time.Duration
	// Code 是 HTTP 状态码；0 表示连接失败或超时
	Code int
	// Problem 是错误响应的 type（rate_limited、quota_exceeded 等）
	Problem string
	// Status 是回答状态（answered、evidence_only、insufficient_evidence、refused）
	Status   string
	Degraded bool
	Dropped  bool
}

func run(ctx context.Context, c *config) (*report, error) {
	start := time.Now()
	var batchID string
	if c.batch > 0 {
		id, err := submitBatch(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("提交批次: %w", err)
		}
		batchID = id
	}
	var (
		mu       sync.Mutex
		samples  []sample
		wg       sync.WaitGroup
		inflight atomic.Int64
		n        int
		mix      = newMixer(c.questions, c.multihop)
	)
	add := func(s sample) {
		mu.Lock()
		samples = append(samples, s)
		mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(ctx, c.duration)
	defer cancel()
	// 开环发送：按目标速率算下一次发出的时刻，到点就发，不等前一个返回
	next := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
		case <-time.After(time.Until(start.Add(next))):
		}
		if ctx.Err() != nil {
			break
		}
		at := time.Since(start)
		kind, q := mix.next()
		token := c.tokens[n%len(c.tokens)]
		n++
		if int(inflight.Load()) >= c.maxInflight {
			add(sample{At: at, Kind: kind, Dropped: true})
		} else {
			inflight.Add(1)
			wg.Go(func() {
				defer inflight.Add(-1)
				s := ask(context.WithoutCancel(ctx), c, token, q)
				s.At, s.Kind = at, kind
				add(s)
			})
		}
		next += time.Duration(float64(time.Second) / c.rateAt(next))
	}
	wg.Wait()
	rep := summarize(samples, c, time.Since(start))
	if batchID != "" {
		rep.Batch = batchStatus(context.Background(), c, batchID)
	}
	return rep, nil
}

func (c *config) request(ctx context.Context, method, path, token string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.base, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if c.tenant != "" {
		req.Header.Set("Bambuflow-Tenant", c.tenant)
	}
	return req, nil
}

// ask 发一次 POST /v1/answers：流式时边读边记第一句到达的时间，以 final 事件结束
func ask(ctx context.Context, c *config, token, q string) sample {
	body, _ := json.Marshal(map[string]any{"question": q, "stream": c.stream})
	req, err := c.request(ctx, http.MethodPost, "/v1/answers", token, bytes.NewReader(body))
	if err != nil {
		return sample{}
	}
	req.Header.Set("Content-Type", "application/json")
	begin := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		return sample{Latency: time.Since(begin)}
	}
	defer func() { _ = resp.Body.Close() }()
	s := sample{Code: resp.StatusCode}
	if resp.StatusCode != http.StatusOK {
		var p struct {
			Type string `json:"type"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&p)
		s.Problem, s.Latency = p.Type, time.Since(begin)
		return s
	}
	if !c.stream {
		var a answerEnd
		if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
			s.Code = 0
		}
		s.Status, s.Degraded, s.Latency = a.Status, len(a.Why.Degraded) > 0, time.Since(begin)
		return s
	}
	err = readSSE(resp.Body, func(event string, data []byte) bool {
		switch event {
		case "sentence":
			if s.First == 0 {
				s.First = time.Since(begin)
			}
		case "final":
			var a answerEnd
			_ = json.Unmarshal(data, &a)
			s.Status, s.Degraded = a.Status, len(a.Why.Degraded) > 0
			return false
		case "error":
			var p struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(data, &p)
			s.Problem = p.Type
			return false
		}
		return true
	})
	s.Latency = time.Since(begin)
	if err != nil || (s.Status == "" && s.Problem == "") {
		// 没收到 final 就断开：回答未完成，按失败计
		s.Code = 0
	}
	return s
}

type answerEnd struct {
	Status string `json:"status"`
	Why    struct {
		Degraded []json.RawMessage `json:"degraded"`
	} `json:"why"`
}

// readSSE 逐个事件回调，fn 返回 false 时停止；注释行（心跳）跳过
func readSSE(r io.Reader, fn func(event string, data []byte) bool) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var event string
	var data bytes.Buffer
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if event != "" || data.Len() > 0 {
				if !fn(event, data.Bytes()) {
					return nil
				}
			}
			event = ""
			data.Reset()
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

// submitBatch 用问题文件里的单跳题拼一个 c.batch 条的批次提交
func submitBatch(ctx context.Context, c *config) (string, error) {
	var lines bytes.Buffer
	qs := c.questions[kindSingle]
	for i := range c.batch {
		b, _ := json.Marshal(map[string]string{"custom_id": fmt.Sprintf("lt-%d", i), "question": qs[i%len(qs)]})
		lines.Write(b)
		lines.WriteByte('\n')
	}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("input_file", "loadtest.jsonl")
	if err != nil {
		return "", err
	}
	if _, err := fw.Write(lines.Bytes()); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}
	req, err := c.request(ctx, http.MethodPost, "/v1/batches", c.tokens[0], &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var b struct {
		BatchID string `json:"batch_id"`
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("返回 %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil || b.BatchID == "" {
		return "", errors.Join(errors.New("响应里没有 batch_id"), err)
	}
	return b.BatchID, nil
}

// batchProgress 是压测结束时批次的进度
type batchProgress struct {
	BatchID string         `json:"batch_id"`
	Status  string         `json:"status"`
	Counts  map[string]int `json:"counts"`
	Error   string         `json:"error,omitempty"`
}

func batchStatus(ctx context.Context, c *config, id string) *batchProgress {
	out := &batchProgress{BatchID: id}
	req, err := c.request(ctx, http.MethodGet, "/v1/batches/"+id, c.tokens[0], nil)
	if err == nil {
		var resp *http.Response
		if resp, err = c.client.Do(req); err == nil {
			defer func() { _ = resp.Body.Close() }()
			err = json.NewDecoder(resp.Body).Decode(out)
		}
	}
	if err != nil {
		out.Error = err.Error()
	}
	return out
}
