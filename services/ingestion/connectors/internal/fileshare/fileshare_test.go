package fileshare

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const meta = `id: fs-rate-notice
title: 利率定价通知
owner: alice
classification: internal
acl: [group:credit-l2]
updated_at: 2026-09-20T00:00:00Z
`

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "rate.pdf", "%PDF-1.7 样例")
	write(t, dir, "rate.pdf"+MetaSuffix, meta)
	write(t, dir, "mail.eml", "Subject: x\n\nbody")
	write(t, dir, "mail.eml"+MetaSuffix, strings.Replace(meta, "fs-rate-notice", "fs-mail", 1))
	files, skipped, err := List(dir)
	if err != nil || len(skipped) != 0 {
		t.Fatal(err, skipped)
	}
	if len(files) != 2 || files[0].Name != "mail.eml" || files[1].Name != "rate.pdf" {
		t.Fatalf("应按文件名排序列出两个文件: %v", files)
	}
	pdf := files[1]
	if pdf.Format.ContentType != "application/pdf" || pdf.Format.SourceType != "file" {
		t.Errorf("格式识别不对: %+v", pdf.Format)
	}
	if strings.Join(pdf.ACLTokens, ",") != "group:credit-l2" || len(pdf.ContentHash) != 64 {
		t.Errorf("元数据或内容摘要不对: %+v", pdf.Meta)
	}
	if files[0].Format.SourceType != "mail" {
		t.Errorf("邮件的来源类型应为 mail: %+v", files[0].Format)
	}
}

// 单个文件不合法时跳过它、记下原因，其余照常返回（入库管道详设第 11 节：单份文档失败不中止整轮）
func TestListSkipsBadFiles(t *testing.T) {
	for name, setup := range map[string]func(dir string){
		"缺元数据文件": func(dir string) { write(t, dir, "a.pdf", "x") },
		"元数据没有对应文件": func(dir string) {
			write(t, dir, "a.pdf"+MetaSuffix, meta)
		},
		"不支持的格式": func(dir string) {
			write(t, dir, "a.exe", "x")
			write(t, dir, "a.exe"+MetaSuffix, meta)
		},
		"空文件": func(dir string) {
			write(t, dir, "a.pdf", "")
			write(t, dir, "a.pdf"+MetaSuffix, meta)
		},
		"元数据有未知字段": func(dir string) {
			write(t, dir, "a.pdf", "x")
			write(t, dir, "a.pdf"+MetaSuffix, meta+"visibility: all\n")
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			setup(dir)
			write(t, dir, "ok.pdf", "%PDF 正常")
			write(t, dir, "ok.pdf"+MetaSuffix, strings.Replace(meta, "fs-rate-notice", "fs-ok", 1))
			files, skipped, err := List(dir)
			if err != nil || len(files) != 1 || files[0].Name != "ok.pdf" || len(skipped) != 1 || skipped[0].Reason == "" {
				t.Fatalf("应跳过坏文件、保留其余: %v %v %v", files, skipped, err)
			}
		})
	}
	// id 重复：先读到的保留，后一个跳过
	dir := t.TempDir()
	write(t, dir, "a.pdf", "x")
	write(t, dir, "a.pdf"+MetaSuffix, meta)
	write(t, dir, "b.pdf", "y")
	write(t, dir, "b.pdf"+MetaSuffix, meta)
	files, skipped, err := List(dir)
	if err != nil || len(files) != 1 || files[0].Name != "a.pdf" || len(skipped) != 1 || skipped[0].Name != "b.pdf" {
		t.Fatalf("id 重复: %v %v %v", files, skipped, err)
	}
	if _, _, err := List(filepath.Join(dir, "nope")); err == nil {
		t.Fatal("目录读不了时整轮失败")
	}
}

func TestOnlyACLChangeKeepsContentHash(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "a.pdf", "%PDF 内容")
	write(t, dir, "a.pdf"+MetaSuffix, meta)
	before, _, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "a.pdf"+MetaSuffix, strings.Replace(meta, "group:credit-l2", "group:risk-dept", 1))
	after, _, err := List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if before[0].ContentHash != after[0].ContentHash || before[0].ACLFingerprint == after[0].ACLFingerprint {
		t.Error("仅 ACL 变化时只应改变权限摘要")
	}
}
