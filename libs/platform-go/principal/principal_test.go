package principal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
)

var now = time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)

// fakeGateway 模拟 gateway 的内部 JWKS 端点，可以轮换密钥、模拟故障，并统计请求次数。
type fakeGateway struct {
	mu    sync.Mutex
	keys  map[string]*ecdsa.PrivateKey
	down  bool
	calls atomic.Int32
	srv   *httptest.Server
}

func newFakeGateway(t *testing.T, kids ...string) *fakeGateway {
	t.Helper()
	g := &fakeGateway{keys: map[string]*ecdsa.PrivateKey{}}
	for _, kid := range kids {
		g.addKey(t, kid)
	}
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		g.calls.Add(1)
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.down {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		var set struct {
			Keys []map[string]string `json:"keys"`
		}
		for kid, k := range g.keys {
			b, _ := k.PublicKey.Bytes() // 0x04 || X || Y
			set.Keys = append(set.Keys, map[string]string{
				"kty": "EC", "crv": "P-256", "use": "sig", "kid": kid,
				"x": base64.RawURLEncoding.EncodeToString(b[1:33]),
				"y": base64.RawURLEncoding.EncodeToString(b[33:]),
			})
		}
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) addKey(t *testing.T, kid string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	g.keys[kid] = k
	g.mu.Unlock()
}

func (g *fakeGateway) removeKey(kid string) {
	g.mu.Lock()
	delete(g.keys, kid)
	g.mu.Unlock()
}

func (g *fakeGateway) setDown(down bool) {
	g.mu.Lock()
	g.down = down
	g.mu.Unlock()
}

func (g *fakeGateway) sign(t *testing.T, kid string, claims jwt.MapClaims) string {
	t.Helper()
	g.mu.Lock()
	key := g.keys[kid]
	g.mu.Unlock()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"sub": "alice", "tenant": "credit", "dept": "信贷部", "title": "客户经理",
		"clearance": "CLASSIFICATION_INTERNAL", "region": "华东", "groups": []string{"credit-l2"},
		"iss": Issuer, "aud": Audience, "jti": "j1",
		"auth_time": now.Add(-time.Minute).Unix(), "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
	}
}

// clock 是测试里可以前移的时钟，Verifier 与 JWKS 共用。
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newVerifier(g *fakeGateway) (*Verifier, *clock) {
	c := &clock{now}
	jwks := NewJWKS(g.srv.URL, g.srv.Client())
	jwks.now = c.now
	v := NewVerifier(jwks)
	v.now = c.now
	return v, c
}

func TestVerifyAcceptsValidPrincipal(t *testing.T) {
	g := newFakeGateway(t, "k1")
	v, _ := newVerifier(g)
	p, err := v.Verify(context.Background(), g.sign(t, "k1", validClaims()))
	if err != nil {
		t.Fatal(err)
	}
	if p.GetSub() != "alice" || p.GetTenant() != "credit" || p.GetDept() != "信贷部" ||
		p.GetClearance() != commonv1.Classification_CLASSIFICATION_INTERNAL || len(p.GetGroups()) != 1 {
		t.Fatalf("声明解析不对: %v", p)
	}
}

func TestEvalIssuerIsSeparate(t *testing.T) {
	g := newFakeGateway(t, "k1")
	online, _ := newVerifier(g)
	replica, _ := newVerifier(g)
	replica.WithIssuer(EvalIssuer)
	evalClaims := validClaims()
	evalClaims["iss"] = EvalIssuer
	// 评测身份只在评测副本上有效；gateway 签发的 Principal 在评测副本上无效
	if _, err := online.Verify(context.Background(), g.sign(t, "k1", evalClaims)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("线上实例应拒绝评测身份: %v", err)
	}
	if _, err := replica.Verify(context.Background(), g.sign(t, "k1", evalClaims)); err != nil {
		t.Fatalf("评测副本应接受评测身份: %v", err)
	}
	if _, err := replica.Verify(context.Background(), g.sign(t, "k1", validClaims())); !errors.Is(err, ErrInvalid) {
		t.Fatalf("评测副本应拒绝 gateway 签发的 Principal: %v", err)
	}
}

func TestVerifyRejects(t *testing.T) {
	g := newFakeGateway(t, "k1")
	other := newFakeGateway(t, "k1") // 同名 kid、不同密钥：模拟伪造签名
	with := func(k string, val any) jwt.MapClaims {
		c := validClaims()
		if val == nil {
			delete(c, k)
		} else {
			c[k] = val
		}
		return c
	}
	cases := map[string]string{
		"过期":         g.sign(t, "k1", with("exp", now.Add(-time.Second).Unix())),
		"签发方不对":      g.sign(t, "k1", with("iss", "someone-else")),
		"受众不对":       g.sign(t, "k1", with("aud", "bambuflow-platform")),
		"有效期超过 5 分钟": g.sign(t, "k1", with("exp", now.Add(6*time.Minute).Unix())),
		"缺少 iat":     g.sign(t, "k1", with("iat", nil)),
		"缺少 tenant":  g.sign(t, "k1", with("tenant", nil)),
		"带平台角色":      g.sign(t, "k1", with("roles", []string{"platform_operator"})),
		"声明类型不符":     g.sign(t, "k1", with("groups", "credit-l2")),
		"未知 kid":     signWithKid(t, other, "k9"),
		"签名不对":       other.sign(t, "k1", validClaims()),
		"HS256 冒充":   hs256(t, validClaims()),
		"alg=none":   none(t, validClaims()),
		"不是 JWT":     "not-a-jwt",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			v, _ := newVerifier(g)
			if _, err := v.Verify(context.Background(), raw); !errors.Is(err, ErrInvalid) {
				t.Fatalf("应拒绝，得到 %v", err)
			}
		})
	}
}

// 管理接口的 Principal 与数据接口的互不通用（gateway 详设 4.2）
func TestOperatorVerifier(t *testing.T) {
	g := newFakeGateway(t, "k1")
	jwks := NewJWKS(g.srv.URL, g.srv.Client())
	c := &clock{now}
	jwks.now = c.now
	v := NewOperatorVerifier(jwks)
	v.now = c.now
	op := validClaims()
	delete(op, "tenant")
	op["roles"] = []string{"platform_operator"}
	p, err := v.Verify(context.Background(), g.sign(t, "k1", op))
	if err != nil || p.GetSub() != "alice" || len(p.GetRoles()) != 1 {
		t.Fatalf("管理身份应通过: %v %v", p, err)
	}
	noRoles := validClaims()
	delete(noRoles, "tenant")
	for name, claims := range map[string]jwt.MapClaims{"数据接口的 Principal": validClaims(), "没有角色": noRoles} {
		if _, err := v.Verify(context.Background(), g.sign(t, "k1", claims)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s 应被拒绝，得到 %v", name, err)
		}
	}
}

func signWithKid(t *testing.T, g *fakeGateway, kid string) string {
	g.addKey(t, kid)
	defer g.removeKey(kid)
	return g.sign(t, kid, validClaims())
}

func hs256(t *testing.T, c jwt.MapClaims) string {
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString([]byte("guessable"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func none(t *testing.T, c jwt.MapClaims) string {
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, c)
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestJWKSUnavailableFailsClosed(t *testing.T) {
	g := newFakeGateway(t, "k1")
	g.setDown(true)
	v, _ := newVerifier(g)
	if _, err := v.Verify(context.Background(), g.sign(t, "k1", validClaims())); !errors.Is(err, ErrInvalid) {
		t.Fatalf("JWKS 不可用且没有缓存时应拒绝，得到 %v", err)
	}
}

func TestKeyRotation(t *testing.T) {
	g := newFakeGateway(t, "k1")
	v, c := newVerifier(g)
	ctx := context.Background()
	if _, err := v.Verify(ctx, g.sign(t, "k1", validClaims())); err != nil {
		t.Fatal(err)
	}

	// 轮换：发布新密钥 k2，新旧并存。刚刷新过不到 30 秒时不会再拉，k2 暂时不认识。
	g.addKey(t, "k2")
	tokK2 := g.sign(t, "k2", validClaims())
	if _, err := v.Verify(ctx, tokK2); err == nil {
		t.Fatal("刷新间隔内不应重新拉取 JWKS")
	}
	c.t = c.t.Add(minRefreshInterval)
	claims := validClaims()
	claims["iat"], claims["exp"] = c.t.Unix(), c.t.Add(5*time.Minute).Unix()
	if _, err := v.Verify(ctx, g.sign(t, "k2", claims)); err != nil {
		t.Fatalf("新 kid 应在刷新后被接受: %v", err)
	}

	// 旧密钥下线：缓存过期后刷新，k1 签的令牌不再被接受。
	later := c.t.Add(maxCacheAge + time.Second)
	claims["iat"], claims["exp"] = later.Unix(), later.Add(5*time.Minute).Unix()
	oldKeyToken := g.sign(t, "k1", claims)
	g.removeKey("k1")
	c.t = later
	if _, err := v.Verify(ctx, oldKeyToken); !errors.Is(err, ErrInvalid) {
		t.Fatalf("已下线的旧密钥签的令牌应被拒绝，得到 %v", err)
	}
	if _, err := v.Verify(ctx, g.sign(t, "k2", claims)); err != nil {
		t.Fatalf("新密钥仍应可用: %v", err)
	}
}

func TestUnknownKidDoesNotHammerJWKS(t *testing.T) {
	g := newFakeGateway(t, "k1")
	v, _ := newVerifier(g)
	ctx := context.Background()
	for range 20 {
		_, _ = v.Verify(ctx, signWithKid(t, g, "forged"))
	}
	if n := g.calls.Load(); n > 1 {
		t.Fatalf("伪造 kid 的请求触发了 %d 次 JWKS 拉取，应限制在 30 秒一次", n)
	}
}
