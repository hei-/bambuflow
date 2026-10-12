package api

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestApprovalRequiresStepUp(t *testing.T) {
	e := newEnv(t)
	e.server.Configs = &fakeConfigs{}
	approve := "/v1/admin/change-requests/chg_000000000000000000000001/approve"
	key := func(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

	for name, tok := range map[string]string{
		"认证强度不够":   e.authz.tokenWith(t, "paul", "console", "1", time.Now()),
		"登录超过时限":   e.authz.tokenWith(t, "paul", "console", "2", time.Now().Add(-11*time.Minute)),
		"令牌没有 acr": e.authz.tokenWith(t, "paul", "console", "", time.Now()),
	} {
		r := e.request(t, http.MethodPost, approve, tok, "", key(name))
		challenge := r.header.Get("WWW-Authenticate")
		if r.status != 401 || r.body["type"] != "insufficient_user_authentication" ||
			!strings.Contains(challenge, `acr_values="2"`) || !strings.Contains(challenge, "max_age=600") {
			t.Fatalf("%s: %d %s %q", name, r.status, r.raw, challenge)
		}
	}
	// 加强认证过：放行（实现方返回变更单已过期）
	if r := e.request(t, http.MethodPost, approve, e.authz.token(t, "paul", "console"), "", key("ok")); r.status != 409 {
		t.Fatalf("加强认证后应放行: %d %s", r.status, r.raw)
	}
	// 非审批的写操作不要求
	weak := e.authz.tokenWith(t, "paul", "console", "1", time.Now().Add(-time.Hour))
	if r := e.request(t, http.MethodPost, "/v1/admin/releases/rel_000000000000000000000001/rollback", weak, `{"reason":"配错了"}`,
		key("rb")); r.status != 200 {
		t.Fatalf("回退不要求加强认证: %d %s", r.status, r.raw)
	}
	// 角色不对先 403，不提示去加强认证
	if r := e.request(t, http.MethodPost, approve, e.authz.tokenWith(t, "audra", "console", "1", time.Now()), "", key("au")); r.status != 403 {
		t.Fatalf("无权限应 403: %d", r.status)
	}
}
