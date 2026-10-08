// Package logging 提供统一的结构化 JSON 日志，并在日志库层脱敏（技术设计文档 12.5）。
//
// 令牌、口令和个人信息不能进入日志（AGENTS.md 第 4 节）。这里做最后一道兜底：按字段名整体替换，
// 再按内容模式替换字符串值和消息里的令牌、手机号、身份证号、银行卡号、邮箱。调用方仍不应主动记录这些内容。
package logging

import (
	"io"
	"log/slog"
	"regexp"
	"strings"
)

const redacted = "[已脱敏]"

// sensitiveKeys 中的字段整体替换，不看内容。比较时忽略大小写和 - _ 的差别。
var sensitiveKeys = map[string]bool{
	"authorization": true, "proxyauthorization": true, "cookie": true, "setcookie": true,
	"token": true, "accesstoken": true, "refreshtoken": true, "idtoken": true,
	"password": true, "passwd": true, "secret": true, "clientsecret": true, "apikey": true,
	"privatekey": true, "question": true, "query": true, "answer": true,
}

type pattern struct {
	re   *regexp.Regexp
	kind string
}

// 顺序有讲究：先替换较长、较具体的模式，避免身份证号被当成手机号截掉一段。
var patterns = []pattern{
	{regexp.MustCompile(`eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*`), "jwt"},
	{regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`), "bearer"},
	{regexp.MustCompile(`\b\d{17}[\dXx]\b`), "id_card"},
	{regexp.MustCompile(`\b\d{16,19}\b`), "bank_card"},
	{regexp.MustCompile(`\b1[3-9]\d{9}\b`), "phone"},
	{regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), "email"},
}

// Redact 把字符串中的敏感内容替换成占位符。
func Redact(s string) string {
	for _, p := range patterns {
		s = p.re.ReplaceAllString(s, "[已脱敏:"+p.kind+"]")
	}
	return s
}

func normalizeKey(k string) string {
	return strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(k))
}

func replaceAttr(_ []string, a slog.Attr) slog.Attr {
	if sensitiveKeys[normalizeKey(a.Key)] {
		return slog.String(a.Key, redacted)
	}
	switch v := a.Value.Resolve(); v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, Redact(v.String()))
	case slog.KindAny:
		// error、Stringer 等最终会以文本输出，先转成字符串再脱敏
		switch x := v.Any().(type) {
		case error:
			return slog.String(a.Key, Redact(x.Error()))
		case interface{ String() string }:
			return slog.String(a.Key, Redact(x.String()))
		}
	}
	return a
}

// New 返回写到 w 的 JSON 日志器，每条日志带 service 字段。
func New(w io.Writer, service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: replaceAttr})
	return slog.New(h).With(slog.String("service", service))
}
