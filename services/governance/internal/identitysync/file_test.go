package identitysync

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fileSyncer(t *testing.T, content string, mod time.Time) (*Syncer, *fakeStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "people.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.Source = "hr-export"
	cfg.File.Path, cfg.File.MaxAge, cfg.File.GroupsColumn, cfg.File.GroupsSeparator = path, 25*time.Hour, "groups", ";"
	store := &fakeStore{}
	s := NewSyncer(cfg, nil, store, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	s.now = func() time.Time { return mod.Add(time.Hour) }
	return s, store, path
}

const export = "\xef\xbb\xbfuid,departmentNumber,employeeType,groups\n" +
	"alice,信贷部,C2,credit-l2;all-staff\n" +
	"bob,风险管理部,C3,all-staff\n"

// 导出文件整份替换：内容变了全量写入（文件里没有的用户停用），没变只记成功。
func TestExportFileReplacesWhenChanged(t *testing.T) {
	mod := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	s, store, path := fileSyncer(t, export, mod)
	ctx := context.Background()
	if err := s.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	b := store.applied[0]
	if !b.Full || len(b.Identities) != 2 || b.UsersCursor == "" {
		t.Fatalf("首次应全量写入两人: %+v", b)
	}
	got := map[string][]string{}
	for _, g := range b.Groups {
		got[g.Name] = g.Members
	}
	if strings.Join(got["all-staff"], ",") != "alice,bob" || strings.Join(got["credit-l2"], ",") != "alice" {
		t.Fatalf("群组按群组列汇总: %v", got)
	}

	if err := s.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if b := store.applied[1]; b.Full || len(b.Identities) != 0 || b.UsersCursor != store.applied[0].UsersCursor {
		t.Fatalf("内容没变只记成功: %+v", b)
	}

	if err := os.WriteFile(path, []byte("uid,departmentNumber,employeeType,groups\nalice,信贷部,C2,credit-l2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(path, mod, mod)
	if err := s.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if b := store.applied[2]; !b.Full || len(b.Identities) != 1 {
		t.Fatalf("内容变了应整份替换（bob 由全量对账停用）: %+v", b)
	}
}

func TestExportFileRejectsStaleOrBadFiles(t *testing.T) {
	mod := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	for name, c := range map[string]struct {
		content string
		age     time.Duration
	}{
		"超过 max_age 没有更新": {export, 26 * time.Hour},
		"没有用户":            {"uid,departmentNumber,employeeType,groups\n", time.Hour},
		"缺少群组列":           {"uid,departmentNumber\nalice,信贷部\n", time.Hour},
		"账号重复":            {"uid,groups\nalice,a\nalice,b\n", time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			s, store, _ := fileSyncer(t, c.content, mod)
			s.now = func() time.Time { return mod.Add(c.age) }
			if err := s.RunOnce(context.Background()); err == nil || len(store.applied) != 0 || len(store.failures) != 1 {
				t.Fatalf("应失败且不写入: %v %+v", err, store.applied)
			}
		})
	}
}

func TestConfigNeedsExactlyOneUpstream(t *testing.T) {
	c := testConfig()
	if err := c.validate(); err == nil {
		t.Fatal("没有配置上游应拒绝")
	}
	c.LDAP.URL, c.LDAP.Users.BaseDN, c.LDAP.Groups.BaseDN = "ldap://x", "ou=p", "ou=g"
	c.LDAP.Groups.NameAttr, c.LDAP.Groups.MemberAttr = "cn", "member"
	if err := c.validate(); err != nil || c.Kind() != KindLDAP {
		t.Fatalf("ldap: %v", err)
	}
	c.SCIM.Listen, c.SCIM.TokenEnv = ":8090", "SCIM_TOKEN"
	if err := c.validate(); err == nil {
		t.Fatal("同时配置两种上游应拒绝")
	}
	c.LDAP.URL = ""
	if err := c.validate(); err != nil || c.Kind() != KindSCIM {
		t.Fatalf("scim: %v", err)
	}
}
