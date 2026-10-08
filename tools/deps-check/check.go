package main

import (
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// 规则来源：技术设计文档 12.2“依赖规则”。用一个检查覆盖 Go 和 Python 两种语言，
// 是为了让规则表只有一份；depguard 和 import-linter 都写不出“任意两个服务之间不互相依赖”
// 这种不点名的规则，要逐对枚举，新增服务时容易漏。

// indexClients 是索引存储（关键词、向量、图谱）的客户端库。只有持有索引凭证的模块能依赖它们。
// 元数据库驱动（pgx、psycopg）不在此列：governance 等模块也要连元数据库，那一层靠 Vault 角色隔离凭证。
var indexClients = map[string][]string{
	"go": {
		"github.com/pgvector/pgvector-go",
		"github.com/opensearch-project/opensearch-go",
		"github.com/elastic/go-elasticsearch",
		"github.com/milvus-io/milvus-sdk-go",
		"github.com/milvus-io/milvus/client",
		"github.com/qdrant/go-client",
		"github.com/neo4j/neo4j-go-driver",
		"github.com/vesoft-inc/nebula-go",
	},
	"py": {"pgvector", "opensearchpy", "elasticsearch", "pymilvus", "qdrant_client", "lancedb", "chromadb", "faiss", "neo4j", "nebula3"},
}

// indexClientOwners 是允许依赖索引客户端的单元。retrieval 的召回插件（如 dense）由 retrieval 宿主，持有同一份读凭证。
var indexClientOwners = []string{"services/retrieval", "services/indexing", "plugins/retrieval/"}

// unit 是一个构建单元：services/、libs/、plugins/ 下带 go.mod 或 pyproject.toml 的目录。
type unit struct {
	rel      string // 相对仓库根目录，如 services/qa/orchestrator
	kind     string // services | libs | plugins
	goModule string
	pyPkgs   []string
}

// host 返回插件的宿主模块名（plugins/<宿主>/...），非插件返回空串。宿主名是服务路径的最后一段，
// 如 plugins/orchestrator 的宿主是 services/qa/orchestrator。
func (u *unit) host() string {
	if u.kind != "plugins" {
		return ""
	}
	return strings.SplitN(u.rel, "/", 3)[1]
}

type violation struct {
	file, imp, reason string
}

func (v violation) String() string { return fmt.Sprintf("%s: 依赖 %s：%s", v.file, v.imp, v.reason) }

// check 在仓库根目录（fsys）上检查依赖规则；路径都是相对根目录、以 / 分隔。
func check(fsys fs.FS) ([]violation, error) {
	units, err := discover(fsys)
	if err != nil {
		return nil, err
	}
	var out []violation
	for _, u := range units {
		err := fs.WalkDir(fsys, u.rel, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if p != u.rel && skipDir(fsys, p, d.Name()) {
					return fs.SkipDir
				}
				return nil
			}
			lang := ""
			switch path.Ext(p) {
			case ".go":
				lang = "go"
			case ".py":
				lang = "py"
			default:
				return nil
			}
			imps, err := imports(fsys, p, lang)
			if err != nil {
				return err
			}
			for _, imp := range imps {
				if reason := judge(u, imp, lang, units); reason != "" {
					out = append(out, violation{p, imp, reason})
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

// skipDir 跳过嵌套的其他单元、测试数据和虚拟环境。
func skipDir(fsys fs.FS, p, name string) bool {
	if name == "testdata" || name == "node_modules" || strings.HasPrefix(name, ".") || name == "__pycache__" {
		return true
	}
	return isUnitDir(fsys, p)
}

func isUnitDir(fsys fs.FS, dir string) bool {
	for _, f := range []string{"go.mod", "pyproject.toml"} {
		if _, err := fs.Stat(fsys, path.Join(dir, f)); err == nil {
			return true
		}
	}
	return false
}

func discover(fsys fs.FS) ([]*unit, error) {
	var units []*unit
	for _, kind := range []string{"services", "libs", "plugins"} {
		if _, err := fs.Stat(fsys, kind); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		err := fs.WalkDir(fsys, kind, func(p string, d fs.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return err
			}
			if d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".") {
				return fs.SkipDir
			}
			if p == kind || !isUnitDir(fsys, p) {
				return nil
			}
			u := &unit{rel: p, kind: kind}
			if kind == "plugins" && strings.Count(u.rel, "/") < 2 {
				return fmt.Errorf("%s: 插件要放在 plugins/<宿主模块>/<插件名>/ 下", u.rel)
			}
			u.goModule = goModulePath(fsys, p)
			u.pyPkgs = pyPackages(fsys, p)
			units = append(units, u)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return units, nil
}

func goModulePath(fsys fs.FS, dir string) string {
	data, err := fs.ReadFile(fsys, path.Join(dir, "go.mod"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`)
		}
	}
	return ""
}

// pyPackages 返回单元提供的顶层 Python 包名（单元根目录或 src/ 下带 __init__.py 的目录）。
func pyPackages(fsys fs.FS, dir string) []string {
	var pkgs []string
	for _, base := range []string{dir, path.Join(dir, "src")} {
		entries, _ := fs.ReadDir(fsys, base)
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if _, err := fs.Stat(fsys, path.Join(base, e.Name(), "__init__.py")); err == nil {
				pkgs = append(pkgs, e.Name())
			}
		}
	}
	return pkgs
}

var (
	pyImport = regexp.MustCompile(`^\s*import\s+(.+)$`)
	pyFrom   = regexp.MustCompile(`^\s*from\s+([\w.]+)\s+import\b`)
)

func imports(fsys fs.FS, p, lang string) ([]string, error) {
	data, err := fs.ReadFile(fsys, p)
	if err != nil {
		return nil, err
	}
	var out []string
	if lang == "go" {
		f, err := parser.ParseFile(token.NewFileSet(), p, data, parser.ImportsOnly)
		if err != nil {
			return nil, err
		}
		for _, s := range f.Imports {
			imp, _ := strconv.Unquote(s.Path.Value)
			out = append(out, imp)
		}
		return out, nil
	}
	// Python 只看行首的 import 语句；相对导入（from . import x）留在包内，不涉及跨单元依赖。
	for _, line := range strings.Split(string(data), "\n") {
		if m := pyFrom.FindStringSubmatch(line); m != nil {
			out = append(out, m[1])
		} else if m := pyImport.FindStringSubmatch(line); m != nil {
			for _, part := range strings.Split(m[1], ",") {
				if name := strings.Fields(part); len(name) > 0 {
					out = append(out, name[0])
				}
			}
		}
	}
	return out, nil
}

func judge(u *unit, imp, lang string, units []*unit) string {
	if isIndexClient(imp, lang) && !ownsIndexClient(u.rel) {
		return "只有 retrieval、indexing（及 retrieval 的召回插件）能依赖索引存储客户端"
	}
	t := owner(imp, lang, units)
	if t == nil || t == u {
		return ""
	}
	switch u.kind {
	case "services":
		switch {
		case t.kind == "services":
			return "服务之间不互相 import，只通过 contracts/ 通信"
		case t.kind == "plugins" && t.host() != path.Base(u.rel):
			return "服务只能编译期注册自己宿主的插件"
		}
	case "libs":
		if t.kind != "libs" {
			return "libs 不能依赖 services 或 plugins"
		}
		if strings.HasPrefix(u.rel, "libs/platform-") {
			return "platform-* 不依赖其他 libs"
		}
	case "plugins":
		if t.kind == "plugins" || (t.kind == "services" && path.Base(t.rel) != u.host()) {
			return "插件只能依赖宿主模块的插件接口和 libs"
		}
	}
	return ""
}

func isIndexClient(imp, lang string) bool {
	for _, c := range indexClients[lang] {
		if imp == c || strings.HasPrefix(imp, c+"/") || strings.HasPrefix(imp, c+".") {
			return true
		}
	}
	return false
}

func ownsIndexClient(rel string) bool {
	for _, o := range indexClientOwners {
		if rel == o || (strings.HasSuffix(o, "/") && strings.HasPrefix(rel, o)) {
			return true
		}
	}
	return false
}

// owner 找出被依赖的仓库内单元：Go 按最长模块路径前缀，Python 按顶层包名。
func owner(imp, lang string, units []*unit) *unit {
	var best *unit
	for _, t := range units {
		if lang == "go" && t.goModule != "" && (imp == t.goModule || strings.HasPrefix(imp, t.goModule+"/")) {
			if best == nil || len(t.goModule) > len(best.goModule) {
				best = t
			}
		}
		if lang == "py" {
			top := strings.SplitN(imp, ".", 2)[0]
			for _, p := range t.pyPkgs {
				if p == top {
					return t
				}
			}
		}
	}
	return best
}
