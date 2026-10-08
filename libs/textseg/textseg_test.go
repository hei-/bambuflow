package textseg

import (
	"slices"
	"testing"
)

func TestTokens(t *testing.T) {
	got := Tokens("个人消费贷款的单户授信额度，不超过３０万元！")
	for _, want := range []string{"个人", "消费", "贷款", "授信", "额度", "30", "万元"} {
		if !slices.Contains(got, want) {
			t.Errorf("缺少 %q，得到 %v", want, got)
		}
	}
	for _, tok := range got {
		if tok == "，" || tok == "！" || tok == " " {
			t.Errorf("标点不应成为词: %v", got)
		}
	}
	// 入库与查询用同一套切分：同一个词两边一致
	if !slices.Equal(Tokens("信贷审批"), Tokens("信贷审批")) || len(Tokens("")) != 0 {
		t.Error("切分应确定且空文本无词")
	}
	if !slices.Contains(Tokens("VPN 双因素认证"), "vpn") {
		t.Error("英文应小写")
	}
}
