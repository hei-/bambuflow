package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func server(t *testing.T, outputs []map[string]any) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"model_name": "bge-m3", "model_version": "1", "outputs": outputs})
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "bge-m3")
}

func TestEmbedReadsDenseAndOptionalSparse(t *testing.T) {
	dense := map[string]any{"name": "embedding", "datatype": "FP32", "shape": []int{2, 2}, "data": []float32{1, 0, 0, 1}}
	sparse := map[string]any{"name": "sparse", "datatype": "BYTES", "shape": []int{2}, "data": []string{`{"7":0.5,"9":0}`, `{}`}}
	vecs, model, err := server(t, []map[string]any{dense, sparse}).Embed(context.Background(), []string{"甲", "乙"})
	if err != nil || model != "bge-m3@1" || len(vecs) != 2 || vecs[1].Dense[1] != 1 {
		t.Fatalf("%v %s %v", vecs, model, err)
	}
	// 权重为 0 的词元去掉；空对象是“有稀疏向量但没有词元”，不是“模型不给”
	if len(vecs[0].Sparse) != 1 || vecs[0].Sparse[7] != 0.5 || vecs[1].Sparse == nil || len(vecs[1].Sparse) != 0 {
		t.Fatalf("稀疏向量: %v", vecs)
	}
	// 只有稠密输出：稀疏向量为 nil
	vecs, _, err = server(t, []map[string]any{dense}).Embed(context.Background(), []string{"甲", "乙"})
	if err != nil || vecs[0].Sparse != nil {
		t.Fatalf("没有稀疏输出: %v %v", vecs, err)
	}
	bad := map[string]any{"name": "sparse", "datatype": "BYTES", "shape": []int{2}, "data": []string{`{"x":1}`, `{}`}}
	if _, _, err := server(t, []map[string]any{dense, bad}).Embed(context.Background(), []string{"甲", "乙"}); err == nil {
		t.Fatal("词元 ID 不合法应报错")
	}
}
