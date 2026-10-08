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
