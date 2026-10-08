package main

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

type term struct {
	bad, good string
}

// loadTerms 读术语表（docs/terminology.md）的表格：第一列是“避免使用”，可用“、”或“/”分隔多个写法。
func loadTerms(fsys fs.FS, name string) ([]term, error) {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	var out []term
	for _, line := range strings.Split(string(data), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 4 || !strings.HasPrefix(line, "|") {
			continue
		}
		bad, good := strings.TrimSpace(cells[1]), strings.TrimSpace(cells[2])
		if bad == "避免使用" || strings.HasPrefix(bad, "---") {
			continue
		}
		for _, b := range strings.FieldsFunc(bad, func(r rune) bool { return r == '、' || r == '/' }) {
			if b = strings.TrimSpace(b); b != "" && b != "—" {
				out = append(out, term{b, good})
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: 没有读到任何“避免使用”的写法，表格格式是否变了", name)
	}
	return out, nil
}

// allowList 来自 allow.txt：
//
//	file <路径或以 / 结尾的目录前缀> [写法]   整个文件（或该文件中的某个写法）不检查
//	context <写法> <包含它的常见词>           这个词里出现的写法不算，如“执行内容”里的“行内”
type allowList struct {
	files    map[string][]string // 路径 → 写法；空切片表示整个文件
	contexts map[string][]string
}

func loadAllow(fsys fs.FS, name string) (allowList, error) {
	a := allowList{map[string][]string{}, map[string][]string{}}
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return a, err
	}
	for i, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		switch {
		case f[0] == "file" && len(f) == 2:
			a.files[f[1]] = []string{}
		case f[0] == "file" && len(f) == 3:
			if old, ok := a.files[f[1]]; !ok || len(old) > 0 {
				a.files[f[1]] = append(old, f[2])
			}
		case f[0] == "context" && len(f) == 3:
			a.contexts[f[1]] = append(a.contexts[f[1]], f[2])
		default:
			return a, fmt.Errorf("%s:%d: 无法解析 %q", name, i+1, line)
		}
	}
	return a, nil
}

// merge 并入另一份白名单。整个文件的豁免优先于只豁免某些写法。
func (a allowList) merge(b allowList) {
	for f, words := range b.files {
		old, ok := a.files[f]
		switch {
		case ok && len(old) == 0:
		case len(words) == 0:
			a.files[f] = []string{}
		default:
			a.files[f] = append(old, words...)
		}
	}
	for w, ctx := range b.contexts {
		a.contexts[w] = append(a.contexts[w], ctx...)
	}
}

// skipped 判断某文件中的某个写法是否列入白名单。
func (a allowList) skipped(file, bad string) bool {
	for p, terms := range a.files {
		if file != p && (!strings.HasSuffix(p, "/") || !strings.HasPrefix(file, p)) {
			continue
		}
		if len(terms) == 0 {
			return true
		}
		for _, t := range terms {
			if t == bad {
				return true
			}
		}
	}
	return false
}

type hit struct {
	file string
	line int
	t    term
}

func (h hit) String() string {
	return fmt.Sprintf("%s:%d: “%s” → 改用“%s”", h.file, h.line, h.t.bad, h.t.good)
}

var textExt = map[string]bool{
	".md": true, ".txt": true, ".go": true, ".py": true, ".ts": true, ".tsx": true, ".proto": true,
	".yaml": true, ".yml": true, ".json": true, ".toml": true, ".html": true, ".rego": true,
}

// check 扫描 fsys（仓库根目录）中的 files，路径相对根目录。
func check(fsys fs.FS, files []string, terms []term, allow allowList) ([]hit, error) {
	var out []hit
	for _, file := range files {
		if !textExt[path.Ext(file)] {
			continue
		}
		data, err := fs.ReadFile(fsys, file)
		if errors.Is(err, fs.ErrNotExist) {
			// 已在工作区删除、尚未提交删除的文件仍会出现在 git 的已跟踪列表里，跳过
			continue
		}
		if err != nil {
			return nil, err
		}
		for n, line := range strings.Split(string(data), "\n") {
			for _, t := range terms {
				if !allow.skipped(file, t.bad) && occurs(line, t.bad, allow.contexts[t.bad]) {
					out = append(out, hit{file, n + 1, t})
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].file != out[j].file {
			return out[i].file < out[j].file
		}
		return out[i].line < out[j].line
	})
	return out, nil
}

// occurs 判断 line 里是否出现 bad，且至少有一处不落在白名单词语里。
func occurs(line, bad string, contexts []string) bool {
	for from := 0; ; {
		i := strings.Index(line[from:], bad)
		if i < 0 {
			return false
		}
		i += from
		if !inContext(line, i, len(bad), contexts) {
			return true
		}
		from = i + len(bad)
	}
}

func inContext(line string, at, n int, contexts []string) bool {
	for _, c := range contexts {
		for from := 0; ; {
			j := strings.Index(line[from:], c)
			if j < 0 {
				break
			}
			j += from
			if j <= at && at+n <= j+len(c) {
				return true
			}
			from = j + 1
		}
	}
	return false
}
