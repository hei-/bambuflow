package store

import "testing"

func TestDecodeSpan(t *testing.T) {
	s, err := decodeSpan([]byte(`{"char_start":3,"char_end":9,"line_start":null,"line_end":null,"page":2,"bbox":[1,2,3,4]}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.GetPage() != 2 || s.GetBbox().GetX1() != 3 || s.GetBbox().GetY1() != 4 || s.LineStart != nil {
		t.Fatalf("解析结果不对: %v", s)
	}
	// P0 写入的块没有 bbox 字段
	s, err = decodeSpan([]byte(`{"char_start":0,"char_end":5,"line_start":1,"line_end":1,"page":null}`))
	if err != nil || s.GetBbox() != nil || s.GetLineStart() != 1 {
		t.Fatalf("没有区域的块: %v %v", s, err)
	}
	if _, err := decodeSpan([]byte(`{"char_start":0,"char_end":5,"bbox":[1,2]}`)); err == nil {
		t.Fatal("坐标个数不对应报错")
	}
}
