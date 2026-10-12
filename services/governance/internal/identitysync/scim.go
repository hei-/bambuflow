package identitysync

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// SCIM 推送端点（gateway 详设 3.5，RFC 7643、7644）：上游目录或 IAM 把用户与群组的变化推送过来，秒级生效。
//
// 支持上游预配（provisioning）常用的部分：用户与群组的新建、查询（filter 只支持 eq）、整体替换（PUT）、
// 修改（PATCH 的 add、replace、remove）与删除；不支持批量、排序、ETag 与改密码。
// 用户的 id 就是平台账号（userName），群组的 id 就是群组名（displayName）：租户配置与策略按群组名引用，
// 改名等于换了一个用户或群组，所以不支持改名（返回 400，上游应先删再建）。删除用户只标记停用，审计和回放要保留历史身份。
// 推送式的上游停推时平台无从察觉：端点在运行、能写库就记同步成功，上游自己的推送失败由 IAM 的预配日志告警。

const (
	scimUser  = "User"
	scimGroup = "Group"

	schemaUser  = "urn:ietf:params:scim:schemas:core:2.0:User"
	schemaGroup = "urn:ietf:params:scim:schemas:core:2.0:Group"
	schemaList  = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
	schemaPatch = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
	schemaError = "urn:ietf:params:scim:api:messages:2.0:Error"

	maxPage   = 1000
	maxSCIMIn = 1 << 20
)

// ErrExists：新建的资源 id 已存在。
var ErrExists = errors.New("资源已存在")

// Resource 是上游推送的一个用户或群组：Body 为最近一次的 SCIM 表示（不含 id 与 meta，由端点生成）。
type Resource struct {
	Type, ID         string
	Body             map[string]any
	Version          int64
	Created, Updated time.Time
}

// Filter 是 GET 列表的条件：Attr 为空表示不过滤。
type Filter struct{ Attr, Value string }

// ModifyFunc 收到锁住的当前资源（不存在时为 nil），返回写回的资源（nil 表示删除）与要写进平台身份库的变化。
type ModifyFunc func(cur *Resource) (*Resource, Batch, error)

// SCIMStore 是 SCIM 资源与平台身份库的读写（PGStore）。
type SCIMStore interface {
	GetResource(ctx context.Context, source, typ, id string) (*Resource, error)
	ListResources(ctx context.Context, source, typ string, f Filter, offset, limit int) ([]*Resource, int, error)
	// ModifyResource 在一个事务里锁住资源、按 fn 写回或删除，并把 fn 给出的变化写进平台身份库
	ModifyResource(ctx context.Context, source, typ, id string, fn ModifyFunc) error
	Touch(ctx context.Context, source string) error
}

type SCIMServer struct {
	cfg   *Config
	store SCIMStore
	token []byte
	log   *slog.Logger
}

// NewSCIMServer 从 cfg.SCIM.TokenEnv 读上游的调用凭证；没有就不启动。
func NewSCIMServer(cfg *Config, store SCIMStore, log *slog.Logger, getenv func(string) string) (*SCIMServer, error) {
	tok := getenv(cfg.SCIM.TokenEnv)
	if len(tok) < 32 {
		return nil, fmt.Errorf("环境变量 %s（SCIM 调用凭证）未设置或短于 32 个字符", cfg.SCIM.TokenEnv)
	}
	return &SCIMServer{cfg: cfg, store: store, token: []byte(tok), log: log}, nil
}

// Run 在 cfg.SCIM.Listen 上提供端点，并每隔 interval 记一次同步成功，直到 ctx 结束。
func (s *SCIMServer) Run(ctx context.Context) error {
	srv := &http.Server{Addr: s.cfg.SCIM.Listen, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go s.heartbeat(ctx)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *SCIMServer) heartbeat(ctx context.Context) {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		if err := s.store.Touch(ctx, s.cfg.Source); err != nil {
			s.log.ErrorContext(ctx, "记录 SCIM 同步状态失败", slog.Any("err", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *SCIMServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /scim/v2/ServiceProviderConfig", s.serviceProviderConfig)
	for _, typ := range []string{scimUser, scimGroup} {
		p := "/scim/v2/" + typ + "s"
		mux.HandleFunc("GET "+p, func(w http.ResponseWriter, r *http.Request) { s.list(w, r, typ) })
		mux.HandleFunc("POST "+p, func(w http.ResponseWriter, r *http.Request) { s.create(w, r, typ) })
		mux.HandleFunc("GET "+p+"/{id}", func(w http.ResponseWriter, r *http.Request) { s.get(w, r, typ) })
		mux.HandleFunc("PUT "+p+"/{id}", func(w http.ResponseWriter, r *http.Request) { s.replace(w, r, typ) })
		mux.HandleFunc("PATCH "+p+"/{id}", func(w http.ResponseWriter, r *http.Request) { s.patch(w, r, typ) })
		mux.HandleFunc("DELETE "+p+"/{id}", func(w http.ResponseWriter, r *http.Request) { s.delete(w, r, typ) })
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), s.token) != 1 {
			scimError(w, http.StatusUnauthorized, "", "缺少或错误的调用凭证")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxSCIMIn)
		mux.ServeHTTP(w, r)
	})
}

// scimErr 是要按 SCIM 错误格式返回给上游的错误。
type scimErr struct {
	status   int
	scimType string
	detail   string
}

func (e *scimErr) Error() string { return e.detail }

func badRequest(scimType, format string, args ...any) error {
	return &scimErr{status: http.StatusBadRequest, scimType: scimType, detail: fmt.Sprintf(format, args...)}
}

func (s *SCIMServer) fail(w http.ResponseWriter, r *http.Request, err error) {
	var se *scimErr
	switch {
	case errors.As(err, &se):
		scimError(w, se.status, se.scimType, se.detail)
	case errors.Is(err, ErrExists):
		scimError(w, http.StatusConflict, "uniqueness", "资源已存在")
	default:
		s.log.ErrorContext(r.Context(), "SCIM 请求处理失败", slog.String("method", r.Method), slog.Any("err", err))
		scimError(w, http.StatusInternalServerError, "", "内部错误")
	}
}

func scimError(w http.ResponseWriter, status int, scimType, detail string) {
	body := map[string]any{"schemas": []string{schemaError}, "status": strconv.Itoa(status), "detail": detail}
	if scimType != "" {
		body["scimType"] = scimType
	}
	writeSCIM(w, status, body)
}

func writeSCIM(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/scim+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *SCIMServer) serviceProviderConfig(w http.ResponseWriter, _ *http.Request) {
	no := map[string]bool{"supported": false}
	writeSCIM(w, http.StatusOK, map[string]any{
		"schemas":        []string{"urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"},
		"patch":          map[string]bool{"supported": true},
		"bulk":           map[string]any{"supported": false, "maxOperations": 0, "maxPayloadSize": 0},
		"filter":         map[string]any{"supported": true, "maxResults": maxPage},
		"changePassword": no, "sort": no, "etag": no,
		"authenticationSchemes": []map[string]any{{
			"type": "oauthbearertoken", "name": "Bearer 令牌", "description": "部署时配置的调用凭证", "primary": true,
		}},
	})
}

var filterRe = regexp.MustCompile(`^\s*(\w+)\s+eq\s+"((?:[^"\\]|\\.)*)"\s*$`)

func (s *SCIMServer) list(w http.ResponseWriter, r *http.Request, typ string) {
	q := r.URL.Query()
	var f Filter
	if raw := q.Get("filter"); raw != "" {
		m := filterRe.FindStringSubmatch(raw)
		allowed := map[string]bool{"id": true, "externalId": true, "userName": typ == scimUser, "displayName": typ == scimGroup}
		if m == nil || !allowed[m[1]] {
			s.fail(w, r, badRequest("invalidFilter", "只支持 id、externalId、%s 的 eq 过滤", idAttr(typ)))
			return
		}
		value, err := strconv.Unquote(`"` + m[2] + `"`)
		if err != nil {
			s.fail(w, r, badRequest("invalidFilter", "过滤值不合法"))
			return
		}
		f = Filter{Attr: m[1], Value: value}
	}
	start, count := 1, 100
	if v := q.Get("startIndex"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 1 {
			start = n
		}
	}
	if v := q.Get("count"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			count = min(n, maxPage)
		}
	}
	items, total, err := s.store.ListResources(r.Context(), s.cfg.Source, typ, f, start-1, count)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		out = append(out, s.represent(it))
	}
	writeSCIM(w, http.StatusOK, map[string]any{
		"schemas": []string{schemaList}, "totalResults": total, "startIndex": start, "itemsPerPage": len(out), "Resources": out,
	})
}

func (s *SCIMServer) get(w http.ResponseWriter, r *http.Request, typ string) {
	res, err := s.store.GetResource(r.Context(), s.cfg.Source, typ, r.PathValue("id"))
	switch {
	case err != nil:
		s.fail(w, r, err)
	case res == nil:
		scimError(w, http.StatusNotFound, "", "资源不存在")
	default:
		writeSCIM(w, http.StatusOK, s.represent(res))
	}
}

func (s *SCIMServer) create(w http.ResponseWriter, r *http.Request, typ string) {
	body, err := readSCIM(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	id, _ := body[idAttr(typ)].(string)
	if id == "" {
		s.fail(w, r, badRequest("invalidValue", "缺少 %s", idAttr(typ)))
		return
	}
	var saved *Resource
	err = s.store.ModifyResource(r.Context(), s.cfg.Source, typ, id, func(cur *Resource) (*Resource, Batch, error) {
		if cur != nil {
			return nil, Batch{}, ErrExists
		}
		next := &Resource{Type: typ, ID: id, Body: body, Version: 1, Created: time.Now(), Updated: time.Now()}
		b, err := s.derive(next, false)
		saved = next
		return next, b, err
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.log.InfoContext(r.Context(), "SCIM 新建", slog.String("type", typ))
	writeSCIM(w, http.StatusCreated, s.represent(saved))
}

func (s *SCIMServer) replace(w http.ResponseWriter, r *http.Request, typ string) {
	body, err := readSCIM(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.modify(w, r, typ, func(cur *Resource) (map[string]any, error) { return body, nil })
}

type patchRequest struct {
	Schemas    []string `json:"schemas"`
	Operations []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	} `json:"Operations"`
}

func (s *SCIMServer) patch(w http.ResponseWriter, r *http.Request, typ string) {
	var req patchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !slices.Contains(req.Schemas, schemaPatch) {
		s.fail(w, r, badRequest("invalidSyntax", "不是 PatchOp 请求"))
		return
	}
	s.modify(w, r, typ, func(cur *Resource) (map[string]any, error) {
		body := cloneJSON(cur.Body)
		for _, op := range req.Operations {
			var v any
			if len(op.Value) > 0 {
				if err := json.Unmarshal(op.Value, &v); err != nil {
					return nil, badRequest("invalidValue", "value 不是合法的 JSON")
				}
			}
			if err := applyOp(body, strings.ToLower(op.Op), op.Path, v); err != nil {
				return nil, err
			}
		}
		return body, nil
	})
}

// modify 锁住已有资源，按 edit 得到新的表示后写回；新表示的账号或群组名必须不变。
func (s *SCIMServer) modify(w http.ResponseWriter, r *http.Request, typ string, edit func(cur *Resource) (map[string]any, error)) {
	id := r.PathValue("id")
	var saved *Resource
	err := s.store.ModifyResource(r.Context(), s.cfg.Source, typ, id, func(cur *Resource) (*Resource, Batch, error) {
		if cur == nil {
			return nil, Batch{}, &scimErr{status: http.StatusNotFound, detail: "资源不存在"}
		}
		body, err := edit(cur)
		if err != nil {
			return nil, Batch{}, err
		}
		if got, _ := body[idAttr(typ)].(string); got != cur.ID {
			return nil, Batch{}, badRequest("mutability", "不支持修改 %s（改名等于换了一个%s，请先删除再新建）", idAttr(typ), typeName(typ))
		}
		next := &Resource{Type: typ, ID: cur.ID, Body: body, Version: cur.Version + 1, Created: cur.Created, Updated: time.Now()}
		b, err := s.derive(next, false)
		saved = next
		return next, b, err
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.log.InfoContext(r.Context(), "SCIM 修改", slog.String("type", typ), slog.String("method", r.Method))
	writeSCIM(w, http.StatusOK, s.represent(saved))
}

func (s *SCIMServer) delete(w http.ResponseWriter, r *http.Request, typ string) {
	err := s.store.ModifyResource(r.Context(), s.cfg.Source, typ, r.PathValue("id"), func(cur *Resource) (*Resource, Batch, error) {
		if cur == nil {
			return nil, Batch{}, &scimErr{status: http.StatusNotFound, detail: "资源不存在"}
		}
		b, err := s.derive(cur, true)
		return nil, b, err
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.log.InfoContext(r.Context(), "SCIM 删除", slog.String("type", typ))
	w.WriteHeader(http.StatusNoContent)
}

// derive 把资源换算成平台身份库的变化。删除用户只标记停用；删除群组清掉它的全部成员关系。
func (s *SCIMServer) derive(res *Resource, deleted bool) (Batch, error) {
	if res.Type == scimGroup {
		g := Group{Name: res.ID}
		if !deleted {
			members, err := memberIDs(res.Body["members"])
			if err != nil {
				return Batch{}, err
			}
			g.Members = members
		}
		return Batch{Groups: []Group{g}}, nil
	}
	attrs := flatten(res.Body)
	id, ok := (&Syncer{cfg: s.cfg, log: s.log}).mapEntry(Entry{DN: "scim:" + res.ID, Attrs: attrs})
	if !ok || id.Username != res.ID {
		return Batch{}, badRequest("invalidValue", "按映射取不到账号（mapping.username = %s）", s.cfg.Mapping.Username)
	}
	// SCIM 的 active 为 false 即停用，不依赖 mapping.status 的配置
	if deleted || attrs["active"] == "false" {
		id.Active = false
	}
	return Batch{Identities: []Identity{id}}, nil
}

func memberIDs(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, badRequest("invalidValue", "members 必须是数组")
	}
	var out []string
	for _, m := range list {
		obj, _ := m.(map[string]any)
		id, _ := obj["value"].(string)
		if id == "" {
			return nil, badRequest("invalidValue", "members 的每一项都要有 value")
		}
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *SCIMServer) represent(res *Resource) map[string]any {
	out := cloneJSON(res.Body)
	delete(out, "password")
	schema := schemaUser
	if res.Type == scimGroup {
		schema = schemaGroup
	}
	if _, ok := out["schemas"]; !ok {
		out["schemas"] = []string{schema}
	}
	out["id"] = res.ID
	out["meta"] = map[string]any{
		"resourceType": res.Type,
		"created":      res.Created.UTC().Format(time.RFC3339),
		"lastModified": res.Updated.UTC().Format(time.RFC3339),
		"location":     "/scim/v2/" + res.Type + "s/" + url.PathEscape(res.ID),
		"version":      fmt.Sprintf(`W/"%d"`, res.Version),
	}
	return out
}

func readSCIM(r *http.Request) (map[string]any, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, badRequest("invalidSyntax", "请求体过大或读取失败")
	}
	var body map[string]any
	if err := json.Unmarshal(b, &body); err != nil {
		return nil, badRequest("invalidSyntax", "请求体不是 JSON 对象")
	}
	// id 与 meta 由端点生成；口令平台不需要，不保存
	delete(body, "id")
	delete(body, "meta")
	delete(body, "password")
	return body, nil
}

func idAttr(typ string) string {
	if typ == scimGroup {
		return "displayName"
	}
	return "userName"
}

func typeName(typ string) string {
	if typ == scimGroup {
		return "群组"
	}
	return "用户"
}

func cloneJSON(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

// flatten 把 SCIM 用户展开成“属性名 → 字符串”，供 mapping 按名取值：顶层属性用原名（userName、title、active），
// 扩展 schema 里的属性用“schema URN:属性”（如 urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:department），
// 复合属性用“属性.子属性”（name.familyName），多值属性取 primary 的一项、没有就取第一项（addresses.region）。
func flatten(body map[string]any) map[string]string {
	out := map[string]string{}
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch x := v.(type) {
		case string:
			out[prefix] = x
		case bool:
			out[prefix] = strconv.FormatBool(x)
		case float64:
			out[prefix] = strconv.FormatFloat(x, 'f', -1, 64)
		case map[string]any:
			for k, sub := range x {
				walk(prefix+"."+k, sub)
			}
		case []any:
			if len(x) == 0 {
				return
			}
			pick := x[0]
			for _, it := range x {
				if obj, ok := it.(map[string]any); ok && obj["primary"] == true {
					pick = it
					break
				}
			}
			walk(prefix, pick)
		}
	}
	for k, v := range body {
		if obj, ok := v.(map[string]any); ok && strings.HasPrefix(k, "urn:") {
			for sub, sv := range obj {
				walk(k+":"+sub, sv)
			}
			continue
		}
		walk(k, v)
	}
	return out
}
