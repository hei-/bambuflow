package identitysync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

type memSCIM struct {
	mu      sync.Mutex
	res     map[string]*Resource
	applied []Batch
	touched int
}

func (m *memSCIM) GetResource(_ context.Context, _, typ, id string) (*Resource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.res[typ+"/"+id], nil
}

func (m *memSCIM) ListResources(_ context.Context, _, typ string, f Filter, offset, limit int) ([]*Resource, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Resource
	for _, k := range slices.Sorted(maps.Keys(m.res)) {
		r := m.res[k]
		if r.Type != typ {
			continue
		}
		if f.Attr == "externalId" && r.Body["externalId"] != f.Value || f.Attr != "" && f.Attr != "externalId" && !strings.EqualFold(r.ID, f.Value) {
			continue
		}
		out = append(out, r)
	}
	total := len(out)
	out = out[min(offset, total):min(offset+limit, total)]
	return out, total, nil
}

func (m *memSCIM) ModifyResource(_ context.Context, _, typ, id string, fn ModifyFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next, b, err := fn(m.res[typ+"/"+id])
	if err != nil {
		return err
	}
	if next == nil {
		delete(m.res, typ+"/"+id)
	} else {
		m.res[typ+"/"+id] = next
	}
	m.applied = append(m.applied, b)
	return nil
}

func (m *memSCIM) Touch(context.Context, string) error { m.touched++; return nil }

const scimToken = "0123456789abcdef0123456789abcdef"

func newSCIM(t *testing.T) (*memSCIM, *httptest.Server) {
	t.Helper()
	cfg := &Config{Source: "corp-scim"}
	cfg.SCIM.Listen, cfg.SCIM.TokenEnv = ":0", "SCIM_TOKEN"
	cfg.Mapping.Username, cfg.Mapping.DisplayName, cfg.Mapping.Title = "userName", "displayName", "title"
	cfg.Mapping.Dept = "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:department"
	cfg.Mapping.Region, cfg.Mapping.Grade = "addresses.region", "urn:corp:params:scim:User:grade"
	cfg.Mapping.ClearanceByGrade = map[string]string{"C2": "internal", "C3": "confidential"}
	store := &memSCIM{res: map[string]*Resource{}}
	srv, err := NewSCIMServer(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(string) string { return scimToken })
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return store, ts
}

func scimDo(t *testing.T, ts *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), method, ts.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+scimToken)
	req.Header.Set("Content-Type", "application/scim+json")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	b, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func lastIdentity(t *testing.T, s *memSCIM) Identity {
	t.Helper()
	b := s.applied[len(s.applied)-1]
	if len(b.Identities) != 1 {
		t.Fatalf("应写入一个用户: %+v", b)
	}
	return b.Identities[0]
}

const alice = `{"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"alice","externalId":"e-1","displayName":"爱丽丝",
"title":"客户经理","active":true,"password":"不保存",
"addresses":[{"type":"home","region":"华南"},{"type":"work","region":"华东","primary":true}],
"urn:ietf:params:scim:schemas:extension:enterprise:2.0:User":{"department":"信贷部"},
"urn:corp:params:scim:User":{"grade":"C3"}}`

// 上游推送用户：按 mapping 写平台身份库；PATCH 停用、改部门立即写入；删除只标记停用。
func TestSCIMUserLifecycle(t *testing.T) {
	store, ts := newSCIM(t)
	status, body := scimDo(t, ts, http.MethodPost, "/scim/v2/Users", alice)
	if status != 201 || body["id"] != "alice" || body["password"] != nil || body["meta"].(map[string]any)["location"] != "/scim/v2/Users/alice" {
		t.Fatalf("新建: %d %v", status, body)
	}
	id := lastIdentity(t, store)
	if !id.Active || id.Dept != "信贷部" || id.Region != "华东" || id.Clearance != 3 || id.Title != "客户经理" || id.DN != "scim:alice" {
		t.Fatalf("映射不对: %+v", id)
	}
	if status, _ := scimDo(t, ts, http.MethodPost, "/scim/v2/Users", alice); status != 409 {
		t.Fatalf("重复新建应 409，得到 %d", status)
	}

	status, body = scimDo(t, ts, http.MethodGet, `/scim/v2/Users?filter=userName+eq+"ALICE"`, "")
	if status != 200 || body["totalResults"] != 1.0 {
		t.Fatalf("按 userName 查（不区分大小写）: %d %v", status, body)
	}
	if status, body = scimDo(t, ts, http.MethodGet, `/scim/v2/Users?filter=title+eq+"x"`, ""); status != 400 || body["scimType"] != "invalidFilter" {
		t.Fatalf("不支持的过滤应 400: %d %v", status, body)
	}

	// Entra 风格：op 首字母大写、不带 path 的 replace，扩展属性写成“URN:属性”
	status, _ = scimDo(t, ts, http.MethodPatch, "/scim/v2/Users/alice", `{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
		"Operations":[{"op":"Replace","value":{"active":false,"urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:department":"风险管理部"}},
		{"op":"replace","path":"addresses[type eq \"work\"].region","value":"华北"}]}`)
	if id := lastIdentity(t, store); status != 200 || id.Active || id.Dept != "风险管理部" || id.Region != "华北" {
		t.Fatalf("PATCH 后: %d %+v", status, id)
	}

	status, body = scimDo(t, ts, http.MethodPatch, "/scim/v2/Users/alice", `{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
		"Operations":[{"op":"replace","path":"userName","value":"alice2"}]}`)
	if status != 400 || body["scimType"] != "mutability" {
		t.Fatalf("改账号应 400: %d %v", status, body)
	}

	if status, _ := scimDo(t, ts, http.MethodDelete, "/scim/v2/Users/alice", ""); status != 204 {
		t.Fatalf("删除: %d", status)
	}
	if id := lastIdentity(t, store); id.Username != "alice" || id.Active {
		t.Fatalf("删除用户应只标记停用: %+v", id)
	}
	if status, _ := scimDo(t, ts, http.MethodGet, "/scim/v2/Users/alice", ""); status != 404 {
		t.Fatalf("删除后查询应 404，得到 %d", status)
	}
}

// 群组成员的增减（Okta、Entra 都用 PATCH members）整组重写 identity_groups。
func TestSCIMGroupMembers(t *testing.T) {
	store, ts := newSCIM(t)
	status, _ := scimDo(t, ts, http.MethodPost, "/scim/v2/Groups", `{"displayName":"credit-l2","members":[{"value":"alice"}]}`)
	if status != 201 {
		t.Fatalf("新建群组: %d", status)
	}
	patch := func(ops string) []string {
		t.Helper()
		status, body := scimDo(t, ts, http.MethodPatch, "/scim/v2/Groups/credit-l2",
			`{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":`+ops+`}`)
		if status != 200 {
			t.Fatalf("PATCH 群组: %d %v", status, body)
		}
		b := store.applied[len(store.applied)-1]
		if len(b.Groups) != 1 || b.Groups[0].Name != "credit-l2" {
			t.Fatalf("应整组重写: %+v", b)
		}
		return b.Groups[0].Members
	}
	if got := patch(`[{"op":"add","path":"members","value":[{"value":"bob"},{"value":"alice"}]}]`); !slices.Equal(got, []string{"alice", "bob"}) {
		t.Fatalf("加成员（去重）: %v", got)
	}
	if got := patch(`[{"op":"remove","path":"members[value eq \"alice\"]"}]`); !slices.Equal(got, []string{"bob"}) {
		t.Fatalf("按过滤移除: %v", got)
	}
	if got := patch(`[{"op":"remove","path":"members","value":[{"value":"bob"}]}]`); len(got) != 0 {
		t.Fatalf("按 value 移除: %v", got)
	}
	if status, _ := scimDo(t, ts, http.MethodDelete, "/scim/v2/Groups/credit-l2", ""); status != 204 {
		t.Fatalf("删除群组: %d", status)
	}
	if b := store.applied[len(store.applied)-1]; len(b.Groups) != 1 || b.Groups[0].Members != nil {
		t.Fatalf("删除群组应清掉全部成员关系: %+v", b)
	}
}

func TestSCIMRequiresToken(t *testing.T) {
	_, ts := newSCIM(t)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/scim/v2/Users", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("错误的凭证应 401，得到 %d", resp.StatusCode)
	}
	cfg := &Config{}
	cfg.SCIM.TokenEnv = "X"
	if _, err := NewSCIMServer(cfg, nil, nil, func(string) string { return "short" }); err == nil {
		t.Fatal("过短的凭证不应启动")
	}
}

func TestPatchPaths(t *testing.T) {
	body := map[string]any{"emails": []any{map[string]any{"type": "work", "value": "a@corp"}}, "Name": map[string]any{"givenName": "A"}}
	for _, c := range []struct{ op, path string }{
		{"add", `emails[type eq "home"].value`},
		{"replace", "name.givenName"},
		{"remove", `emails[type eq "work"]`},
	} {
		if err := applyOp(body, c.op, c.path, "x"); err != nil {
			t.Fatalf("%s %s: %v", c.op, c.path, err)
		}
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	if got := strings.TrimSpace(buf.String()); got != `{"Name":{"givenName":"x"},"emails":[{"type":"home","value":"x"}]}` {
		t.Fatalf("得到 %s", got)
	}
	if err := applyOp(body, "add", "emails.value", "x"); err == nil {
		t.Fatal("多值属性不带过滤条件取子属性应拒绝")
	}
	if err := applyOp(body, "move", "title", "x"); err == nil {
		t.Fatal("未知 op 应拒绝")
	}
}
