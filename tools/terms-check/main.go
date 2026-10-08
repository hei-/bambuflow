// terms-check 按术语表扫描仓库文本，报出“避免使用”的写法（AGENTS.md 第 8 节）。
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

func main() {
	root := flag.String("root", ".", "仓库根目录")
	glossary := flag.String("terms", "docs/terminology.md", "术语表，相对 root")
	allowPath := flag.String("allow", "tools/terms-check/allow.txt", "白名单，相对 root")
	flag.Parse()

	if err := run(*root, *glossary, *allowPath); err != nil {
		fmt.Fprintln(os.Stderr, "terms-check:", err)
		os.Exit(2)
	}
}

func run(root, glossary, allowPath string) error {
	fsys := os.DirFS(root)
	terms, err := loadTerms(fsys, glossary)
	if err != nil {
		return err
	}
	allow, err := loadAllow(fsys, allowPath)
	if err != nil {
		return err
	}
	// 只在私有仓库里的文件（开源导出时不带）的白名单单独放，存在时一并读取
	private := path.Join(path.Dir(allowPath), "allow.private.txt")
	if _, err := fs.Stat(fsys, private); err == nil {
		more, err := loadAllow(fsys, private)
		if err != nil {
			return err
		}
		allow.merge(more)
	}
	// 已跟踪和尚未提交的文件都查，忽略 .gitignore 里的生成物。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "ls-files", "--cached", "--others", "--exclude-standard")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("git ls-files: %w", err)
	}
	hits, err := check(fsys, strings.Fields(string(out)), terms, allow)
	if err != nil {
		return err
	}
	for _, h := range hits {
		fmt.Println(h)
	}
	if len(hits) > 0 {
		fmt.Fprintf(os.Stderr, "terms-check: %d 处使用了术语表中“避免使用”的写法\n", len(hits))
		os.Exit(1)
	}
	return nil
}
