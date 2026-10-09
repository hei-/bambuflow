package search

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

type fakeRestorer struct{ err error }

func (f fakeRestorer) Restore(_ context.Context, chunkID, text string, _ []byte, _, _ string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return "还原后：" + chunkID, nil
}

func TestTextRestoresOnlyForPIIViewers(t *testing.T) {
	s := &Server{PII: fakeRestorer{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	c := Candidate{ChunkID: "c1", Text: "手机 138****5678", PII: SealedPII{Ciphertext: []byte("x"), WrappedKey: "w", KeyID: "k"}}
	ctx := context.Background()
	if got := s.text(ctx, Filter{ShowPII: true}, c); got != "还原后：c1" {
		t.Fatalf("PII 查看者应看到原值: %q", got)
	}
	if got := s.text(ctx, Filter{}, c); got != c.Text {
		t.Fatalf("其他人只看掩码: %q", got)
	}
	// 解不开时退回掩码正文（fail-closed）
	s.PII = fakeRestorer{err: errors.New("vault down")}
	if got := s.text(ctx, Filter{ShowPII: true}, c); got != c.Text {
		t.Fatalf("解不开应返回掩码: %q", got)
	}
	// 没有密文的块原样返回；非 PII 查看者对未掩码的原文仍做第二道掩码
	if got := s.text(ctx, Filter{}, Candidate{Text: "手机 13812345678"}); got != "手机 138****5678" {
		t.Fatalf("第二道掩码: %q", got)
	}
}
