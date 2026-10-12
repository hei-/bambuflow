package roles

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "roles.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAndOf(t *testing.T) {
	m, err := Load(write(t, "roles:\n  platform_operator: [platform-ops]\n  auditor: [platform-audit, sec-audit]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Of([]string{"all-staff", "sec-audit", "platform-ops"}); !slices.Equal(got, []string{Auditor, PlatformOperator}) {
		t.Fatalf("角色不对: %v", got)
	}
	if got := m.Of([]string{"all-staff"}); len(got) != 0 {
		t.Fatalf("不在任何角色群组里应没有角色: %v", got)
	}
}

func TestLoadRejectsUnknownRole(t *testing.T) {
	if _, err := Load(write(t, "roles:\n  superuser: [root]\n")); err == nil {
		t.Fatal("未知角色应报错")
	}
	m, err := Load(write(t, "roles: {}\n"))
	if err != nil || len(m.Of([]string{"x"})) != 0 {
		t.Fatalf("空映射应允许: %v", err)
	}
}

func TestAllowsScopedRoles(t *testing.T) {
	have := []string{Scoped(TenantOwner, "credit")}
	if !Allows(have, []string{PlatformAdmin, TenantOwner}) {
		t.Fatal("带作用域的角色按名字匹配")
	}
	if Allows(have, []string{PlatformAdmin}) || Allows([]string{"tenant_owner_x"}, []string{TenantOwner}) {
		t.Fatal("不应匹配")
	}
}
