package main

import (
	"strings"
	"testing"
	"testing/fstest"
)

// tree 按 路径 → 内容 生成一棵内存中的仓库。
func tree(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for p, c := range files {
		fsys[p] = &fstest.MapFile{Data: []byte(c)}
	}
	return fsys
}

const mod = "github.com/bambu-ai/bambuflow"

func goMod(path string) string { return "module " + mod + "/" + path + "\n\ngo 1.26.0\n" }

func TestCheck(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
		want  []string // 每条违规里应出现的片段；空表示应当通过
	}{
		{
			name: "服务依赖 libs 和契约是允许的",
			files: map[string]string{
				"services/gateway/go.mod":        goMod("services/gateway"),
				"services/gateway/main.go":       "package main\nimport (\n_ \"" + mod + "/libs/platform-go/auth\"\n_ \"" + mod + "/contracts/gen/go/answer/v1\"\n)\n",
				"libs/platform-go/go.mod":        goMod("libs/platform-go"),
				"libs/platform-go/auth/a.go":     "package auth\n",
				"services/retrieval/go.mod":      goMod("services/retrieval"),
				"services/retrieval/main.go":     "package main\nimport _ \"github.com/pgvector/pgvector-go\"\n",
				"plugins/retrieval/dense/go.mod": goMod("plugins/retrieval/dense"),
				"plugins/retrieval/dense/d.go":   "package dense\nimport _ \"github.com/qdrant/go-client/qdrant\"\nimport _ \"" + mod + "/services/retrieval/plugin\"\n",
			},
		},
		{
			name: "反例：Go 服务 import 另一个服务",
			files: map[string]string{
				"services/gateway/go.mod":   goMod("services/gateway"),
				"services/gateway/main.go":  "package main\nimport _ \"" + mod + "/services/retrieval/internal/search\"\n",
				"services/retrieval/go.mod": goMod("services/retrieval"),
			},
			want: []string{"services/gateway/main.go: 依赖 " + mod + "/services/retrieval/internal/search：服务之间不互相"},
		},
		{
			name: "反例：orchestrator 引入索引客户端",
			files: map[string]string{
				"services/qa/orchestrator/pyproject.toml":               "[project]\nname = \"orchestrator\"\n",
				"services/qa/orchestrator/src/orchestrator/__init__.py": "",
				"services/qa/orchestrator/src/orchestrator/fast.py":     "import os, pymilvus as pm\n",
			},
			want: []string{"fast.py: 依赖 pymilvus：只有 retrieval、indexing"},
		},
		{
			name: "反例：Python 服务 import 另一个服务的包（evaluation 不 import orchestrator）",
			files: map[string]string{
				"services/qa/orchestrator/pyproject.toml":               "",
				"services/qa/orchestrator/src/orchestrator/__init__.py": "",
				"services/evaluation/pyproject.toml":                    "",
				"services/evaluation/src/evaluation/__init__.py":        "",
				"services/evaluation/src/evaluation/run.py":             "from orchestrator.graph import answer\nfrom . import util\n",
			},
			want: []string{"run.py: 依赖 orchestrator.graph：服务之间不互相"},
		},
		{
			name: "反例：libs 反向依赖服务、platform 依赖其他 libs",
			files: map[string]string{
				"libs/grounding/pyproject.toml":                     "",
				"libs/grounding/grounding/__init__.py":              "import orchestrator\n",
				"libs/platform-py/pyproject.toml":                   "",
				"libs/platform-py/platform_py/__init__.py":          "from grounding import nli\n",
				"services/qa/orchestrator/pyproject.toml":           "",
				"services/qa/orchestrator/orchestrator/__init__.py": "",
			},
			want: []string{
				"grounding/__init__.py: 依赖 orchestrator：libs 不能依赖",
				"platform_py/__init__.py: 依赖 grounding：platform-* 不依赖其他 libs",
			},
		},
		{
			name: "反例：插件依赖非宿主服务",
			files: map[string]string{
				"plugins/connectors/wiki/go.mod": goMod("plugins/connectors/wiki"),
				"plugins/connectors/wiki/w.go":   "package wiki\nimport _ \"" + mod + "/services/governance/api\"\n",
				"services/governance/go.mod":     goMod("services/governance"),
			},
			want: []string{"w.go: 依赖 " + mod + "/services/governance/api：插件只能依赖宿主"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vs, err := check(tree(c.files))
			if err != nil {
				t.Fatal(err)
			}
			if len(vs) != len(c.want) {
				t.Fatalf("违规数 = %d，期望 %d：%v", len(vs), len(c.want), vs)
			}
			for i, w := range c.want {
				if !strings.Contains(vs[i].String(), w) {
					t.Errorf("第 %d 条 = %q，应包含 %q", i, vs[i], w)
				}
			}
		})
	}
}

func TestPluginLayout(t *testing.T) {
	root := tree(map[string]string{"plugins/wiki/go.mod": goMod("plugins/wiki")})
	if _, err := check(root); err == nil || !strings.Contains(err.Error(), "plugins/<宿主模块>/<插件名>/") {
		t.Fatalf("插件目录层级不对时应报错，得到 %v", err)
	}
}

func TestHostedPlugins(t *testing.T) {
	root := tree(map[string]string{
		"services/qa/orchestrator/pyproject.toml":            "",
		"services/qa/orchestrator/orchestrator/__init__.py":  "import sql_tool\n",
		"services/gateway/go.mod":                            goMod("services/gateway"),
		"services/gateway/main.go":                           "package main\nimport _ \"" + mod + "/plugins/connectors/wiki\"\n",
		"plugins/orchestrator/sql-tool/pyproject.toml":       "",
		"plugins/orchestrator/sql-tool/sql_tool/__init__.py": "",
		"plugins/connectors/wiki/go.mod":                     goMod("plugins/connectors/wiki"),
	})
	vs, err := check(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || !strings.Contains(vs[0].String(), "services/gateway/main.go") || !strings.Contains(vs[0].reason, "自己宿主的插件") {
		t.Fatalf("只应报 gateway 引入非自身宿主的插件，得到 %v", vs)
	}
}
