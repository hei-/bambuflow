// Package textseg 是关键词检索用的中文分词（技术设计文档 12.7：PostgreSQL 全文检索加中文分词）。
//
// 入库（indexing 写 tsvector）与查询（retrieval 拼 tsquery）必须用同一个分词器和同一套归一化，
// 否则同一个词在两边切得不一样就永远匹配不上，所以放在 libs 里共用。
// 不用数据库侧的分词扩展（zhparser、pg_jieba）：那需要自己编译 PostgreSQL 镜像，换后端时也带不走。
package textseg

import (
	"strings"
	"sync"
	"unicode"

	"github.com/go-ego/gse"
	"golang.org/x/text/unicode/norm"
)

var (
	once sync.Once
	seg  gse.Segmenter
	err  error
)

// Load 加载内嵌词典（约 1 秒），进程启动时调用一次；之后 Tokens 可以并发使用。
func Load() error {
	once.Do(func() {
		// 用编进二进制的简体中文词典：gse.New() 会按 Go 模块路径从磁盘读词典，测试时能读到，
		// 镜像里没有这些文件（CI run 37280464157 中 indexing 因此启动失败）
		seg, err = gse.NewEmbed("zh_s")
	})
	return err
}

// Tokens 把文本切成检索用的词：先做 NFKC 归一化和小写，再按搜索模式切分（长词同时给出其中的短词），
// 去掉标点与空白。保留重复的词：词频参与排序。
func Tokens(text string) []string {
	if Load() != nil {
		return nil
	}
	text = strings.ToLower(norm.NFKC.String(text))
	var out []string
	for _, t := range seg.CutSearch(text, true) {
		t = strings.TrimSpace(t)
		if t != "" && strings.IndexFunc(t, isWordRune) >= 0 {
			out = append(out, t)
		}
	}
	return out
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}
