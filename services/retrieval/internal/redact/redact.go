// Package redact 对正文做 PII 掩码（技术设计文档 8.2、8.6）：身份证号、手机号、银行卡号、邮箱。
//
// 与 orchestrator 的 Python 实现规则相同，两边以 tools/test-upstream/pii_cases.json 为共同样例。
// 掩码是幂等的：已掩码的文本不会再被识别，引文核对以掩码后的正文为准，不会因重复掩码而对不上。
package redact

import (
	"regexp"
	"strings"
)

var (
	// 18 位身份证：前 6 位地区码、8 位出生日期、3 位顺序码、1 位校验码（可为 X）
	idCard = regexp.MustCompile(`(^|[^0-9])([1-9][0-9]{5}(?:19|20)[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])[0-9]{3}[0-9Xx])($|[^0-9Xx])`)
	mobile = regexp.MustCompile(`(^|[^0-9])(1[3-9][0-9]{9})($|[^0-9])`)
	card   = regexp.MustCompile(`(^|[^0-9])([0-9]{16,19})($|[^0-9])`)
	email  = regexp.MustCompile(`([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Za-z]{2,})`)
)

// Mask 返回掩码后的文本。
func Mask(text string) string {
	text = replaceDigits(idCard, text, func(s string) string { return s[:6] + strings.Repeat("*", 8) + s[14:] })
	text = replaceDigits(card, text, func(s string) string {
		if !luhn(s) {
			return s
		}
		return s[:4] + strings.Repeat("*", len(s)-8) + s[len(s)-4:]
	})
	text = replaceDigits(mobile, text, func(s string) string { return s[:3] + "****" + s[7:] })
	return email.ReplaceAllString(text, "${1}***@${2}")
}

// replaceDigits 替换模式里第 2 个分组（号码本身），前后的边界字符原样保留。
// Go 的正则不支持环视，相邻两个号码共用一个边界字符时第二个会漏掉，所以循环到不再变化为止。
func replaceDigits(re *regexp.Regexp, text string, mask func(string) string) string {
	for {
		out := re.ReplaceAllStringFunc(text, func(m string) string {
			sub := re.FindStringSubmatch(m)
			return sub[1] + mask(sub[2]) + sub[3]
		})
		if out == text {
			return out
		}
		text = out
	}
}

// luhn 校验银行卡号，避免把流水号等普通长数字当成卡号。
func luhn(s string) bool {
	sum := 0
	double := false
	for i := len(s) - 1; i >= 0; i-- {
		d := int(s[i] - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}
