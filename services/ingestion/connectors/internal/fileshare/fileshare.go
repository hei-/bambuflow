// Package fileshare 读取共享盘样例源（P1-T2）：PDF、Office、邮件、图片等文件。
//
// 共享盘本身不给出制度文件的属主、密级与 ACL，样例里用旁挂的元数据文件 <文件名>.meta.yaml 模拟
// 文档管理系统接口返回的这些字段，内容与 Wiki 的 front matter 相同（见 docmeta）。
// 原文按字节原样交给 processing 解析；内容摘要取原文字节的 sha256。
package fileshare

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/docmeta"
)

// MetaSuffix 是旁挂元数据文件的后缀。
const MetaSuffix = ".meta.yaml"

// MaxBytes 是单个文件的上限。技术设计文档 5.5：超过 100 MB 的文件走单独队列，P1 还没有这条队列，先拒绝。
const MaxBytes = 100 << 20

// Format 是一种文件格式在入库链路上的约定：processing 按 ContentType 选择解析器，SourceType 写入引用。
type Format struct {
	ContentType string
	SourceType  string // file | mail | image | table（对应 common.v1.SourceType）
}

// Formats 按扩展名列出支持的格式。不在表里的文件整批拒绝，而不是悄悄跳过。
var Formats = map[string]Format{
	".pdf":  {"application/pdf", "file"},
	".docx": {"application/vnd.openxmlformats-officedocument.wordprocessingml.document", "file"},
	".xlsx": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "table"},
	".eml":  {"message/rfc822", "mail"},
	".png":  {"image/png", "image"},
	".jpg":  {"image/jpeg", "image"},
}

type File struct {
	docmeta.Meta
	Name        string // 文件名，source_uri 用它定位原文
	Format      Format
	Raw         []byte
	ContentHash string
}

// List 读取目录下的全部文件及其元数据；任一文件不合法就整体失败，不发出半批事件。
func List(dir string) ([]*File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for _, e := range entries {
		if e.Type().IsRegular() {
			names[e.Name()] = true
		}
	}
	var files []string
	for n := range names {
		if base, ok := strings.CutSuffix(n, MetaSuffix); ok {
			if !names[base] {
				return nil, fmt.Errorf("%s 没有对应的文件", n)
			}
			continue
		}
		files = append(files, n)
	}
	sort.Strings(files)
	// 只在样例目录内读取：文件名来自目录列表，os.Root 再保证不会经符号链接或 .. 读到目录之外
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	seen := map[string]string{}
	out := make([]*File, 0, len(files))
	for _, n := range files {
		f, err := read(root, n)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", n, err)
		}
		if prev, dup := seen[f.DocID]; dup {
			return nil, fmt.Errorf("%s 与 %s 的 id 重复", n, prev)
		}
		seen[f.DocID] = n
		out = append(out, f)
	}
	return out, nil
}

func read(root *os.Root, name string) (*File, error) {
	format, ok := Formats[strings.ToLower(filepath.Ext(name))]
	if !ok {
		return nil, fmt.Errorf("不支持的格式 %q", filepath.Ext(name))
	}
	metaRaw, err := root.ReadFile(name + MetaSuffix)
	if err != nil {
		return nil, fmt.Errorf("缺少元数据文件 %s%s", name, MetaSuffix)
	}
	meta, err := docmeta.Decode(metaRaw)
	if err != nil {
		return nil, err
	}
	info, err := root.Stat(name)
	if err != nil {
		return nil, err
	}
	if info.Size() > MaxBytes {
		return nil, fmt.Errorf("文件 %d 字节，超过 %d 字节的上限", info.Size(), MaxBytes)
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("空文件")
	}
	raw, err := root.ReadFile(name)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return &File{Meta: *meta, Name: name, Format: format, Raw: raw, ContentHash: hex.EncodeToString(sum[:])}, nil
}
