package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

const prompt = "<evidence ref=\"S1\" title=\"贷款\">\n# 第二章\n第五条 单户额度不超过 30 万元。其余略\n</evidence>\n" +
	"<evidence ref=\"S2\" title=\"手册\">\n短句。\n员工请假须提前一天在系统中提交申请。\n</evidence>\n"

func lines(t *testing.T, content string) []unit {
	t.Helper()
	var out []unit
	for _, l := range strings.Split(content, "\n") {
		var u unit
		if err := json.Unmarshal([]byte(l), &u); err != nil {
			t.Fatalf("不是 JSON 行: %q", l)
		}
		out = append(out, u)
	}
	return out
}

func TestAnswerQuotesEvidence(t *testing.T) {
	content, status := answer(prompt + "<question>\n额度？\n</question>")
	if status != http.StatusOK {
		t.Fatal(status)
	}
	us := lines(t, content)
	if len(us) != 2 || us[0].Quote[0] != "第五条 单户额度不超过 30 万元。" || us[0].Text != us[0].Quote[0]+"[S1]" {
		t.Fatalf("应从每条证据摘第一句够长的非标题句: %+v", us)
	}
	if us[1].Refs[0] != 2 || us[1].Quote[0] != "员工请假须提前一天在系统中提交申请。" {
		t.Fatalf("第二条证据: %+v", us[1])
	}
}

func TestMergeKeepsGroupsAndPoints(t *testing.T) {
	got := merge("<groups>\n<group id=\"g1\" title=\"主要内容\">\np1：甲。\np2：乙。\n</group>\n" +
		"<group id=\"g2\" title=\"额度\">\np3：丙。\n</group>\n</groups>\n<question>\n总结\n</question>")
	want := `{"points":["p1","p2"],"title":"主要内容"}` + "\n" + `{"points":["p3"],"title":"额度"}`
	if got != want {
		t.Fatalf("合并输出:\n%s", got)
	}
}

func TestScenarios(t *testing.T) {
	content, _ := answer(prompt + "<question>\n额度？[mock:ungrounded][mock:fake-quote]\n</question>")
	us := lines(t, content)
	if len(us) != 4 || us[2].Text != "所有客户均可享受三年免息优惠。[S1]" || us[3].Quote[0] != "贷款期限最长为三十年。" {
		t.Fatalf("样例行: %+v", us)
	}
	if _, status := answer(prompt + "<question>\n[mock:error]\n</question>"); status != http.StatusServiceUnavailable {
		t.Fatalf("error 样例应返回 503，得到 %d", status)
	}
}

func TestStreamReassemblesToSameContent(t *testing.T) {
	content, _ := answer(prompt + "<question>\n额度？\n</question>")
	rec := httptest.NewRecorder()
	writeStream(rec, content, nil)
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type: %s", ct)
	}
	var got strings.Builder
	pieces, done := 0, false
	sc := bufio.NewScanner(rec.Body)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			done = true
			continue
		}
		var chunk struct {
			Model   string `json:"model"`
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil || chunk.Model != model {
			t.Fatalf("数据块不合法: %q", data)
		}
		if c := chunk.Choices[0].Delta.Content; c != "" {
			pieces++
			got.WriteString(c)
		}
	}
	if !done || got.String() != content {
		t.Fatalf("拼回的正文应与非流式一致，done=%v\n%s", done, got.String())
	}
	if pieces <= len(strings.Split(content, "\n")) {
		t.Errorf("每行应跨多个数据块，得到 %d 块", pieces)
	}
}

func TestPlan(t *testing.T) {
	content, status := plan("<question>\n新旧两版有什么区别？[mock-plan:旧版的抵押率|新版的抵押率][mock:ungrounded]\n</question>")
	if status != http.StatusOK || content != `{"id":"q1","text":"旧版的抵押率"}`+"\n"+`{"id":"q2","text":"新版的抵押率"}` {
		t.Fatalf("应按标记拆子问题: %d %q", status, content)
	}
	if content, _ := plan("<question>\n额度是多少？[mock:ungrounded]\n</question>"); content != `{"id":"q1","text":"额度是多少？"}` {
		t.Fatalf("没有规划标记时原问题去掉标记即唯一的子问题: %q", content)
	}
	content, _ = plan("<seen>\n贷款：略\n</seen>\n<pending>\nq2：新版的抵押率\n</pending>\n<question>\n区别？\n</question>")
	if content != `{"id":"q2","text":"新版的抵押率 相关规定"}` {
		t.Fatalf("第 2 轮只为未覆盖的子问题换表述: %q", content)
	}
	if _, status := plan("<question>\n区别？[mock:plan-error]\n</question>"); status != http.StatusServiceUnavailable {
		t.Fatalf("plan-error 样例应返回 503，得到 %d", status)
	}
}

func TestPlanSQL(t *testing.T) {
	q := `余额合计多少？[mock-sql:{"view":"bankdemo.v_loan_balance","measures":[{"agg":"sum","column":"balance"}]}]`
	content, status := plan("<views>\n目录\n</views>\n<question>\n" + q + "\n</question>")
	want := `{"id":"q1","text":"余额合计多少？","tool":"sql_query","args":{"view":"bankdemo.v_loan_balance","measures":[{"agg":"sum","column":"balance"}]}}`
	if status != http.StatusOK || content != want {
		t.Fatalf("应规划成 sql_query: %d %q", status, content)
	}
	// 没有 <views>（租户没开放 sql_query）时按普通问题拆
	if content, _ := plan("<question>\n" + q + "\n</question>"); strings.Contains(content, "sql_query") {
		t.Fatalf("没有视图目录时不应规划 sql_query: %q", content)
	}
}

// do 发一个请求，返回响应（调用方关闭 Body）。
func do(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// setFaults 设置（n > 0）或清除（n = 0）注入的故障。
func setFaults(t *testing.T, srv *httptest.Server, n int) {
	t.Helper()
	resp := do(t, http.MethodPost, srv.URL+"/mock/faults", fmt.Sprintf(`{"fail_next":%d}`, n))
	if n == 0 {
		_ = resp.Body.Close()
		resp = do(t, http.MethodDelete, srv.URL+"/mock/faults", "")
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("设置故障: %d", resp.StatusCode)
	}
}

// post 向模拟网关发一次流式补全，返回状态码与最后一个带 usage 的数据块里的用量。
func post(t *testing.T, srv *httptest.Server, includeUsage bool) (int, map[string]int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"model": model, "stream": true, "stream_options": map[string]bool{"include_usage": includeUsage},
		"messages": []map[string]string{{"role": "system", "content": "系统"}, {"role": "user", "content": prompt + "<question>\n额度？\n</question>"}},
	})
	resp := do(t, http.MethodPost, srv.URL+"/v1/chat/completions", string(body))
	defer func() { _ = resp.Body.Close() }()
	var usage map[string]int
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []any          `json:"choices"`
			Usage   map[string]int `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("数据块不合法: %q", data)
		}
		if chunk.Usage != nil {
			if len(chunk.Choices) != 0 {
				t.Fatalf("usage 应单独成块: %q", data)
			}
			usage = chunk.Usage
		}
	}
	return resp.StatusCode, usage
}

func TestStreamUsageIsDeterministic(t *testing.T) {
	srv := httptest.NewServer(newMux(&recorder{}, &faults{}))
	defer srv.Close()
	status, usage := post(t, srv, true)
	content, _ := answer(prompt + "<question>\n额度？\n</question>")
	in := utf8.RuneCountInString("系统") + utf8.RuneCountInString(prompt+"<question>\n额度？\n</question>")
	if status != http.StatusOK || usage["prompt_tokens"] != in || usage["completion_tokens"] != utf8.RuneCountInString(content) {
		t.Fatalf("用量应为全部消息与正文的字数: %d %v", status, usage)
	}
	if _, again := post(t, srv, true); again["prompt_tokens"] != in || again["completion_tokens"] != usage["completion_tokens"] {
		t.Fatalf("同样的请求用量应相同: %v %v", usage, again)
	}
	if _, none := post(t, srv, false); none != nil {
		t.Fatalf("没有请求 include_usage 时不发 usage: %v", none)
	}
}

func TestFaultsFailNextRequests(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(newMux(rec, &faults{}))
	defer srv.Close()
	setFaults(t, srv, 2)
	var got []int
	for range 3 {
		status, _ := post(t, srv, true)
		got = append(got, status)
	}
	if got[0] != http.StatusServiceUnavailable || got[1] != http.StatusServiceUnavailable || got[2] != http.StatusOK {
		t.Fatalf("前 2 次应失败、之后恢复: %v", got)
	}
	if n := rec.count("额度"); n != 3 {
		t.Fatalf("注入故障的请求也应计数: %d", n)
	}
	setFaults(t, srv, 5)
	setFaults(t, srv, 0)
	if status, _ := post(t, srv, true); status != http.StatusOK {
		t.Fatalf("清除后应恢复: %d", status)
	}
}
