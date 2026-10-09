package incremental

import (
	"fmt"
	"slices"
	"testing"
)

func TestDiff(t *testing.T) {
	state := map[string]string{}
	for i := range 40 {
		state[fmt.Sprintf("p%02d", i)] = "h1|a1"
	}
	current := map[string]string{}
	for k, v := range state {
		current[k] = v
	}
	current["p00"] = "h2|a1" // 内容变了
	current["p01"] = "h1|a2" // 只有权限变了
	current["new"] = "h1|a1" // 新增
	delete(current, "p39")   // 源端删除

	p := Diff(state, current, false)
	if !slices.Equal(p.Upserts, []string{"new", "p00"}) || !slices.Equal(p.AclOnly, []string{"p01"}) ||
		!slices.Equal(p.Deletes, []string{"p39"}) || p.DeletesPaused {
		t.Fatalf("计划 %+v", p)
	}
	// 没有变化：什么都不发
	if p := Diff(state, state, false); len(p.Upserts)+len(p.Deletes) != 0 {
		t.Fatalf("无变化时不应有变更: %+v", p)
	}
}

func TestDeleteProtection(t *testing.T) {
	state := map[string]string{}
	for i := range 40 {
		state[fmt.Sprintf("p%02d", i)] = "h"
	}
	// 清单为空：不推断删除
	if p := Diff(state, map[string]string{}, false); !p.DeletesPaused || p.Reason != "empty_listing" || p.Deletes != nil || p.Held != nil {
		t.Fatalf("空清单 %+v", p)
	}
	// 一次删 3 份（超过 5%）：暂停，等确认
	current := map[string]string{}
	for i := 3; i < 40; i++ {
		current[fmt.Sprintf("p%02d", i)] = "h"
	}
	// 拦下的待删文档留给删除保护的批次（internal/holds）
	if p := Diff(state, current, false); !p.DeletesPaused || p.Reason != "over_threshold" || p.Deletes != nil ||
		!slices.Equal(p.Held, []string{"p00", "p01", "p02"}) {
		t.Fatalf("超阈值 %+v", p)
	}
	if p := Diff(state, current, true); p.DeletesPaused || len(p.Deletes) != 3 {
		t.Fatalf("确认后应删除 %+v", p)
	}
}
