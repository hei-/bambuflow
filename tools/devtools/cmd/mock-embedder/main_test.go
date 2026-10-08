package main

import (
	"math"
	"testing"
)

func cosine(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func TestEmbedDeterministicNormalizedAndLexicallyMeaningful(t *testing.T) {
	a := embed("个人授信额度的计算方法")
	if len(a) != dim {
		t.Fatalf("维度 %d", len(a))
	}
	if n := cosine(a, a); math.Abs(n-1) > 1e-5 {
		t.Errorf("应归一化，|v|²=%f", n)
	}
	if cosine(a, embed("个人授信额度的计算方法")) < 0.9999 {
		t.Error("同一文本应得到同一向量")
	}
	near, far := cosine(a, embed("授信额度怎么计算")), cosine(a, embed("反洗钱可疑交易报告流程"))
	if near <= far {
		t.Errorf("字面相近的文本应更相似: near=%.3f far=%.3f", near, far)
	}
}

func TestOverlap(t *testing.T) {
	if overlap("授信额度", "单户授信额度不超过三十万元") != 1 || overlap("请假", "授信额度") != 0 {
		t.Error("模拟精排分应为查询二元组在段落中的占比")
	}
}
