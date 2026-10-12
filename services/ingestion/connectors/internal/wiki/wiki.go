// Package wiki 读取 Wiki 样例源。
//
// 样例页面是带 YAML front matter 的 Markdown：front matter 模拟源系统接口返回的元数据与 ACL
// （校验与归一化见 docmeta），正文交给 processing 解析。
package wiki

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
)

type Page struct {
	docmeta.Meta
	Body        string // 去掉 front matter 后的正文；原文位置都以它为准
	ContentHash string
}

// Parse 解析一个页面文件。
func Parse(raw []byte) (*Page, error) {
	raw = bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	if !bytes.HasPrefix(raw, []byte("---\n")) {
		return nil, errors.New("缺少 front matter")
	}
	end := bytes.Index(raw[4:], []byte("\n---\n"))
	if end < 0 {
		return nil, errors.New("front matter 没有结束标记")
	}
	meta, err := docmeta.Decode(raw[4 : 4+end])
	if err != nil {
		return nil, err
	}
	body := string(raw[4+end+5:])
	sum := sha256.Sum256([]byte(body))
	return &Page{Meta: *meta, Body: body, ContentHash: hex.EncodeToString(sum[:])}, nil
}

// List 读取目录下的全部 .md 页面。单个页面不合法（缺 front matter、元数据错、id 重复）时跳过并记进 skipped，
// 其余照常返回；读不了目录才返回错误。
func List(dir string) ([]*Page, []docmeta.Skipped, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(paths)
	seen := map[string]string{}
	var out []*Page
	var skipped []docmeta.Skipped
	for _, p := range paths {
		name := filepath.Base(p)
		raw, err := os.ReadFile(filepath.Clean(p))
		if err != nil {
			skipped = append(skipped, docmeta.Skipped{Name: name, Reason: err.Error()})
			continue
		}
		page, err := Parse(raw)
		if err != nil {
			skipped = append(skipped, docmeta.Skipped{Name: name, Reason: err.Error()})
			continue
		}
		if prev, dup := seen[page.DocID]; dup {
			skipped = append(skipped, docmeta.Skipped{Name: name, Reason: fmt.Sprintf("与 %s 的 id 重复", prev)})
			continue
		}
		seen[page.DocID] = name
		out = append(out, page)
	}
	return out, skipped, nil
}
