package main

import "testing"

func TestSupport(t *testing.T) {
	quote := "个人消费贷款单户额度原则上不超过 30 万元。"
	if s := support(quote, quote); s != 1 { // grounding 送来的假设句已去掉 [S#] 标记
		t.Errorf("照抄引文应为 1，得到 %.2f", s)
	}
	if s := support(quote, "所有客户均可享受三年免息优惠。"); s >= 0.5 {
		t.Errorf("无关句子应低于阈值 0.5，得到 %.2f", s)
	}
	if s := support(quote, "。。"); s != 0 {
		t.Errorf("没有内容的句子应为 0，得到 %.2f", s)
	}
}

func TestContradiction(t *testing.T) {
	quote := "个人消费贷款单户额度原则上不超过 30 万元。"
	cases := []struct {
		hypothesis string
		want       bool
	}{
		{quote, false},
		{"个人消费贷款单户额度原则上不超过 20 万元。", true}, // 同一件事、数字不同
		{"所有客户均可享受 3 年免息优惠。", false},      // 说的不是同一件事
		{"个人消费贷款单户额度不超过三十万元。", false},     // 假设句没有数字
	}
	for _, c := range cases {
		if got := contradiction(quote, c.hypothesis) > 0.5; got != c.want {
			t.Errorf("%q：矛盾 = %v，应为 %v", c.hypothesis, got, c.want)
		}
	}
}
