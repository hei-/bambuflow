package identitysync

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

// LDAPDirectory 从 LDAP / Active Directory 按修改时间增量读取用户与群组。
type LDAPDirectory struct {
	cfg      *Config
	password string
}

// LDAP 的修改时间是 GeneralizedTime（如 20261004011500Z），作为过滤条件拼接前先校验格式。
var generalizedTime = regexp.MustCompile(`^\d{14}(\.\d+)?Z$`)

const pageSize = 500

func NewLDAPDirectory(cfg *Config) (*LDAPDirectory, error) {
	pw := os.Getenv(cfg.LDAP.BindPasswordEnv)
	if cfg.LDAP.BindDN != "" && pw == "" {
		return nil, fmt.Errorf("未设置环境变量 %s（LDAP 绑定口令）", cfg.LDAP.BindPasswordEnv)
	}
	return &LDAPDirectory{cfg: cfg, password: pw}, nil
}

func (d *LDAPDirectory) search(ctx context.Context, base, filter, since string, attrs []string) ([]*ldap.Entry, error) {
	if since != "" {
		if !generalizedTime.MatchString(since) {
			return nil, fmt.Errorf("游标格式不对: %q", since)
		}
		filter = "(&" + filter + "(modifyTimestamp>=" + since + "))"
	}
	conn, err := ldap.DialURL(d.cfg.LDAP.URL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		conn.SetTimeout(timeUntil(dl))
	}
	if d.cfg.LDAP.BindDN != "" {
		if err := conn.Bind(d.cfg.LDAP.BindDN, d.password); err != nil {
			return nil, err
		}
	}
	req := ldap.NewSearchRequest(base, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		filter, append(attrs, "modifyTimestamp"), nil)
	res, err := conn.SearchWithPaging(req, pageSize)
	if err != nil {
		return nil, err
	}
	return res.Entries, nil
}

func (d *LDAPDirectory) Users(ctx context.Context, since string) ([]Entry, error) {
	m := d.cfg.Mapping
	attrs := nonEmpty(m.Username, m.DisplayName, m.Dept, m.Title, m.Region, m.Grade, m.Status.Attr)
	entries, err := d.search(ctx, d.cfg.LDAP.Users.BaseDN, d.cfg.LDAP.Users.Filter, since, attrs)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		vals := map[string]string{}
		for _, a := range attrs {
			vals[a] = e.GetAttributeValue(a)
		}
		out = append(out, Entry{DN: e.DN, Attrs: vals, Modified: e.GetAttributeValue("modifyTimestamp")})
	}
	return out, nil
}

func (d *LDAPDirectory) Groups(ctx context.Context, since string) ([]Group, error) {
	g := d.cfg.LDAP.Groups
	entries, err := d.search(ctx, g.BaseDN, g.Filter, since, []string{g.NameAttr, g.MemberAttr})
	if err != nil {
		return nil, err
	}
	out := make([]Group, 0, len(entries))
	for _, e := range entries {
		grp := Group{Name: e.GetAttributeValue(g.NameAttr), Modified: e.GetAttributeValue("modifyTimestamp")}
		for _, member := range e.GetAttributeValues(g.MemberAttr) {
			if u := usernameFromDN(member, d.cfg.Mapping.Username); u != "" {
				grp.Members = append(grp.Members, u)
			}
		}
		out = append(out, grp)
	}
	return out, nil
}

// usernameFromDN 从成员 DN 的第一个 RDN 取账号（如 uid=alice,ou=people,... → alice）。
// RDN 不是账号属性的成员（如嵌套群组）不在 P0 范围，忽略。
func usernameFromDN(dn, attr string) string {
	parsed, err := ldap.ParseDN(dn)
	if err != nil || len(parsed.RDNs) == 0 {
		return ""
	}
	for _, a := range parsed.RDNs[0].Attributes {
		if strings.EqualFold(a.Type, attr) {
			return a.Value
		}
	}
	return ""
}

func nonEmpty(xs ...string) []string {
	var out []string
	for _, x := range xs {
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}
