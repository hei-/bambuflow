package denyrule

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"
)

func rule() Rule {
	return Rule{ID: "den_abc", Scope: "source", Target: "src_wiki", Reason: "工单 SEC-1", IssuedBy: "ivan",
		IssuedAt: time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)}
}

func TestSignAndVerify(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	signed, err := Sign(key, rule())
	if err != nil {
		t.Fatal(err)
	}
	v, _ := NewVerifier(&key.PublicKey)
	got, err := v.Verify(signed)
	if err != nil || got != rule() {
		t.Fatalf("验签: %v %+v", err, got)
	}

	// 改了内容（例如把拒绝的对象换掉）签名就不对
	var e envelope
	_ = json.Unmarshal(signed, &e)
	e.Payload = []byte(`{"id":"den_abc","scope":"tenant","target":"credit","reason":"x","issued_by":"m","issued_at":"2026-10-11T01:00:00Z"}`)
	tampered, _ := json.Marshal(e)
	if _, err := v.Verify(tampered); err == nil {
		t.Fatal("篡改过的规则应拒绝")
	}
	// 别的密钥签的不受信任
	forged, _ := Sign(other, rule())
	if _, err := v.Verify(forged); err == nil {
		t.Fatal("不受信任的密钥签的规则应拒绝")
	}
	bad := rule()
	bad.Target = "wiki"
	if _, err := Sign(key, bad); err == nil {
		t.Fatal("字段不合法的规则不应签发")
	}
}
