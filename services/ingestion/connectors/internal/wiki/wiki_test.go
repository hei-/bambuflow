package wiki

import (
	"strings"
	"testing"
)

const page = `---
id: credit-policy
title: 个人授信管理办法
owner: alice
classification: internal
acl: [group:credit-l2, user:bob, group:credit-l2]
updated_at: 2026-09-01T08:00:00Z
---
# 个人授信管理办法

## 第一章 总则
`

func TestParse(t *testing.T) {
	p, err := Parse([]byte(page))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.ACLTokens, ",") != "group:credit-l2,user:bob" {
		t.Errorf("ACL 应去重排序: %v", p.ACLTokens)
	}
	if !strings.HasPrefix(p.Body, "# 个人授信管理办法") {
		t.Errorf("正文应从 front matter 之后开始: %q", p.Body[:20])
	}
	if len(p.ContentHash) != 64 || len(p.ACLFingerprint) != 64 {
		t.Error("应计算内容摘要与权限摘要")
	}
	// 只改 ACL：内容摘要不变，权限摘要变化
	q, _ := Parse([]byte(strings.Replace(page, "user:bob", "user:carl", 1)))
	if q.ContentHash != p.ContentHash || q.ACLFingerprint == p.ACLFingerprint {
		t.Error("仅 ACL 变化时只应改变权限摘要")
	}
}

func TestMissingACLIsOwnerOnly(t *testing.T) {
	p, err := Parse([]byte(strings.Replace(page, "acl: [group:credit-l2, user:bob, group:credit-l2]\n", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.ACLTokens, ",") != "user:alice" {
		t.Errorf("没有 ACL 时只给属主: %v", p.ACLTokens)
	}
	p, err = Parse([]byte(strings.NewReplacer("acl: [group:credit-l2, user:bob, group:credit-l2]\n", "", "owner: alice\n", "").Replace(page)))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.ACLTokens) != 0 {
		t.Errorf("没有 ACL 也没有属主时谁都不可见: %v", p.ACLTokens)
	}
}

func TestParseRejects(t *testing.T) {
	for name, raw := range map[string]string{
		"没有 front matter": "# 标题\n",
		"非法 ACL":          strings.Replace(page, "user:bob", "everyone", 1),
		"非法密级":            strings.Replace(page, "internal", "secret", 1),
		"未知字段":            strings.Replace(page, "owner: alice", "owner: alice\nvisibility: all", 1),
		"缺 updated_at":    strings.Replace(page, "updated_at: 2026-09-01T08:00:00Z\n", "", 1),
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s 应报错", name)
		}
	}
}
