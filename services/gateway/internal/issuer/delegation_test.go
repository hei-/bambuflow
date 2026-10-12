package issuer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/hei-/bambuflow/libs/platform-go/principal"
)

func newIssuer(t *testing.T) *Issuer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	i, err := New(k)
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func delegation(now time.Time) Delegation {
	return Delegation{Username: "alice", Tenant: "credit", Actor: "console", BatchID: "bat_01", AuthTime: 7, Expires: now.Add(7 * 24 * time.Hour)}
}

func TestDelegationRoundTrip(t *testing.T) {
	i := newIssuer(t)
	raw, err := i.IssueDelegation(delegation(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	d, err := i.VerifyDelegation(raw, "bat_01")
	if err != nil {
		t.Fatal(err)
	}
	if d.Username != "alice" || d.Tenant != "credit" || d.Actor != "console" || d.AuthTime != 7 {
		t.Fatalf("委托内容不对: %+v", d)
	}
}

func TestDelegationRejected(t *testing.T) {
	i := newIssuer(t)
	now := time.Now()
	raw, err := i.IssueDelegation(delegation(now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.VerifyDelegation(raw, "bat_02"); !errors.Is(err, ErrDelegationInvalid) {
		t.Errorf("别的批次不能用这份委托，得到 %v", err)
	}
	if _, err := newIssuer(t).VerifyDelegation(raw, "bat_01"); !errors.Is(err, ErrDelegationInvalid) {
		t.Errorf("别的密钥签发的委托应拒绝，得到 %v", err)
	}
	if _, err := i.VerifyDelegation(raw[:len(raw)-4]+"AAAA", "bat_01"); !errors.Is(err, ErrDelegationInvalid) {
		t.Errorf("篡改过的委托应拒绝，得到 %v", err)
	}
	// 过期：把签发器的时钟拨到 8 天后
	i.now = func() time.Time { return now.Add(8 * 24 * time.Hour) }
	if _, err := i.VerifyDelegation(raw, "bat_01"); !errors.Is(err, ErrDelegationInvalid) {
		t.Errorf("过期的委托应拒绝，得到 %v", err)
	}
}

func TestDelegationLifetimeCapped(t *testing.T) {
	i := newIssuer(t)
	d := delegation(time.Now())
	d.Expires = time.Now().Add(8 * 24 * time.Hour)
	if _, err := i.IssueDelegation(d); err == nil {
		t.Fatal("超过 7 天的委托不应签发")
	}
}

type staticKey struct{ i *Issuer }

func (k staticKey) Key(context.Context, string) (any, error) {
	return &k.i.keys.Load().signing.priv.PublicKey, nil
}

// 委托和 Principal 用同一把密钥签名，但受众不同：下游不能把委托当成 Principal 接受。
func TestDelegationIsNotAPrincipal(t *testing.T) {
	i := newIssuer(t)
	raw, err := i.IssueDelegation(delegation(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := principal.NewVerifier(staticKey{i}).Verify(context.Background(), raw); err == nil {
		t.Fatal("委托不应通过 Principal 校验")
	}
}
