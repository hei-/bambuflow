package writer

import (
	"slices"
	"testing"
	"time"
)

func TestReceiptProtoListsPendingStores(t *testing.T) {
	at := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	done := map[string]time.Time{"graph": at, "sections": at, "manifests": at, "chunks": at, "metadata": at}
	r := receiptProto("del_1", "d1", "src_wiki", nil, done, at)
	if !slices.Equal(r.GetPendingStores(), []string{"backups", "object_store", "llm_traces"}) || len(r.GetStores()) != 5 || r.GetStores()[0].GetStore() != "chunks" {
		t.Fatalf("备份密钥环、对象存储与 LLM 追踪库还没删完: %v", r)
	}
	done["backups"], done["object_store"], done["llm_traces"] = at, at, at
	if r := receiptProto("del_1", "d1", "src_wiki", &at, done, at); len(r.GetPendingStores()) != 0 || r.GetTombstonedAt() == nil {
		t.Fatalf("全部完成: %v", r)
	}
	if _, ok := receiptOffset("!!"); ok {
		t.Fatal("非法的翻页标记")
	}
	if n, ok := receiptOffset(""); !ok || n != 0 {
		t.Fatal("第一页")
	}
}
