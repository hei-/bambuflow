package search

import (
	"context"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func pinned(v string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(EvalIndexHeader, v))
}

func TestEvalPinOnlyInEvalMode(t *testing.T) {
	called := false
	handler := func(context.Context, any) (any, error) { called = true; return nil, nil }
	// 线上实例：带了指定索引版本就拒绝，处理函数根本不执行
	if _, err := EvalPinInterceptor(false)(pinned("ix-2"), nil, &grpc.UnaryServerInfo{}, handler); status.Code(err) != codes.PermissionDenied || called {
		t.Fatalf("线上实例应拒绝: %v called=%v", err, called)
	}
	if _, err := EvalPinInterceptor(false)(context.Background(), nil, &grpc.UnaryServerInfo{}, handler); err != nil || !called {
		t.Fatalf("不带时照常处理: %v", err)
	}

	st := &fakeStore{}
	s := &Server{Store: st}
	if _, _, err := s.indexVersion(pinned("ix-2")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("没经过拦截器也不能放过: %v", err)
	}
	s.EvalMode = true
	v, m, err := s.indexVersion(pinned("ix-2"))
	if err != nil || v != "ix-2" || m != model || !slices.Contains(st.calls, "VersionModel:ix-2") {
		t.Fatalf("%s %s %v %v", v, m, err, st.calls)
	}
	if _, _, err := s.indexVersion(pinned("ix-gone")); status.Code(err) != codes.NotFound {
		t.Fatalf("已丢弃的版本: %v", err)
	}
	if _, _, err := s.indexVersion(pinned("a b")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("版本号不合法: %v", err)
	}
	if v, _, err := s.indexVersion(context.Background()); err != nil || v != "ix-1" {
		t.Fatalf("不指定时读 prod: %s %v", v, err)
	}
}
