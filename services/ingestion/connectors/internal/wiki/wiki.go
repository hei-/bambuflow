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

	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/docmeta"
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

// List 读取目录下的全部 .md 页面；任一页面不合法就整体失败，不发出半批事件。
func List(dir string) ([]*Page, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	seen := map[string]string{}
	var out []*Page
	for _, p := range paths {
		raw, err := os.ReadFile(filepath.Clean(p))
		if err != nil {
			return nil, err
		}
		page, err := Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		if prev, dup := seen[page.DocID]; dup {
			return nil, fmt.Errorf("%s 与 %s 的 id 重复", filepath.Base(p), prev)
		}
		seen[page.DocID] = filepath.Base(p)
		out = append(out, page)
	}
	return out, nil
}
