package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/hei-/bambuflow/services/gateway/internal/authn"
)

const certHeader = "Ssl-Client-Subject-Dn"

// tokenAct 是业务系统经平台授权服务做令牌交换取得的令牌：azp 与 act.sub 都是业务系统
func (a *fakeAuthz) tokenAct(t *testing.T, username, client, act string) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": platformIssuer, "aud": authn.Audience, "azp": client, "preferred_username": username,
		"iat": now.Unix(), "exp": now.Add(15 * time.Minute).Unix(), "auth_time": now.Unix(), "acr": "1",
		"act": map[string]string{"sub": act},
	})
	tok.Header["kid"] = "platform-k1"
	s, err := tok.SignedString(a.key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// 业务系统代用户调用：令牌经交换取得（带 act），请求还要带同一业务系统的客户端证书（gateway 详设 3.1）。
func TestOnBehalfRequiresMatchingServiceIdentity(t *testing.T) {
	e := newEnv(t)
	e.server.ClientCertHeader = certHeader
	exchanged := e.authz.tokenAct(t, "alice", "biz-credit", "biz-credit")
	cert := func(dn string) map[string]string { return map[string]string{certHeader: dn} }
	cases := []struct {
		name, token string
		headers     map[string]string
		status      int
		typ         string
	}{
		{"没有客户端证书", exchanged, nil, 401, "unauthenticated"},
		{"证书是别的业务系统", exchanged, cert("CN=biz-risk,O=Corp"), 401, "unauthenticated"},
		{"证书主题有两个 CN", exchanged, cert("CN=biz-credit+CN=biz-risk,O=Corp"), 401, "unauthenticated"},
		{"act 与 azp 不一致", e.authz.tokenAct(t, "alice", "biz-credit", "biz-risk"), cert("CN=biz-risk"), 401, "unauthenticated"},
		{"平台级客户端不做令牌交换", e.authz.tokenAct(t, "alice", "console", "console"), cert("CN=console"), 403, "forbidden"},
		{"MCP agent 不做令牌交换", e.authz.tokenAct(t, "alice", "credit-bot", "credit-bot"), cert("CN=credit-bot"), 403, "forbidden"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := "/v1/search"
			if strings.Contains(c.name, "MCP") {
				path = "/mcp"
			}
			r := e.request(t, http.MethodPost, path, c.token, q, c.headers)
			if r.status != c.status || r.body["type"] != c.typ {
				t.Fatalf("得到 %d %v，期望 %d %s", r.status, r.body["type"], c.status, c.typ)
			}
		})
	}

	r := e.request(t, http.MethodPost, "/v1/search", exchanged, q, cert("CN=biz-credit,OU=信贷系统,O=Corp"))
	if r.status != 200 || r.body["index_version"] != "alice@credit|CLASSIFICATION_INTERNAL|groups=1|ref=|actor=biz-credit" {
		t.Fatalf("证书与 act 一致时应以用户身份放行、actor 为业务系统: %d %v", r.status, r.body["index_version"])
	}

	// 没配置证书请求头的部署不接受代用户调用
	e.server.ClientCertHeader = ""
	if r := e.request(t, http.MethodPost, "/v1/search", exchanged, q, cert("CN=biz-credit")); r.status != 401 {
		t.Fatalf("未配置服务身份时应拒绝，得到 %d", r.status)
	}
}

func TestCommonName(t *testing.T) {
	for dn, want := range map[string]string{
		"CN=crm-app,O=Corp":      "crm-app",
		" cn = crm-app , O=Corp": "crm-app",
		`CN=a\,b,O=Corp`:         "a,b",
		"O=Corp":                 "",
		"CN=a,CN=b":              "",
		"":                       "",
	} {
		if got := commonName(dn); got != want {
			t.Errorf("commonName(%q) = %q，期望 %q", dn, got, want)
		}
	}
}
