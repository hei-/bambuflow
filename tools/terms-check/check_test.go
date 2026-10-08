package main

import (
	"os"
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	fsys := os.DirFS("testdata")
	terms, err := loadTerms(fsys, "terminology.md")
	if err != nil {
		t.Fatal(err)
	}
	allow, err := loadAllow(fsys, "allow.txt")
	if err != nil {
		t.Fatal(err)
	}
	files := []string{"docs/bad.md", "docs/ok.md", "docs/rules.md", "examples/a.md", "img.png", "docs/deleted.md"}
	hits, err := check(fsys, files, terms, allow)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, h := range hits {
		got = append(got, h.String())
	}
	want := []string{
		"docs/bad.md:1: “金集” → 改用“评测集”",
		"docs/bad.md:2: “晚绑定” → 改用“索引时权限过滤 / 查询时实时鉴权”",
		"docs/bad.md:3: “行内” → 改用“企业级、公司内部”",
		"docs/rules.md:2: “墓碑” → 改用“删除标记”",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("得到\n%s\n期望\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestLoadTermsRejectsEmptyTable(t *testing.T) {
	if _, err := loadTerms(os.DirFS("testdata"), "allow.txt"); err == nil {
		t.Fatal("没有表格时应报错，避免术语表格式变化后检查静默放行")
	}
}

func TestMergeAllowLists(t *testing.T) {
	a := allowList{map[string][]string{"a.md": {"金集"}, "b.md": {}}, map[string][]string{"行内": {"执行内"}}}
	b := allowList{map[string][]string{"a.md": {"墓碑"}, "b.md": {"摄取"}, "c.md": {}}, map[string][]string{"行内": {"运行内"}}}
	a.merge(b)
	if !a.skipped("a.md", "金集") || !a.skipped("a.md", "墓碑") || a.skipped("a.md", "摄取") {
		t.Fatalf("a.md 应合并两份写法: %v", a.files["a.md"])
	}
	if !a.skipped("b.md", "任何写法") || !a.skipped("c.md", "任何写法") {
		t.Fatalf("整个文件的豁免应保留: %v", a.files)
	}
	if len(a.contexts["行内"]) != 2 {
		t.Fatalf("上下文应合并: %v", a.contexts)
	}
}
