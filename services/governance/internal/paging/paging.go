// Package paging 是 governance 管理接口共用的分页参数：page_size 0 取 50、上限 200；
// page_token 是下一页的起点（不透明，调用方原样带回）。
package paging

import (
	"encoding/base64"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Parse 返回每页条数与起点。
func Parse(size int32, token string) (int, int, error) {
	n := int(size)
	if n == 0 {
		n = 50
	}
	if n < 1 || n > 200 {
		return 0, 0, status.Error(codes.InvalidArgument, "page_size 应为 1–200")
	}
	if token == "" {
		return n, 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, 0, status.Error(codes.InvalidArgument, "page_token 不合法")
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(string(raw), "o:"))
	if err != nil || offset < 0 || !strings.HasPrefix(string(raw), "o:") {
		return 0, 0, status.Error(codes.InvalidArgument, "page_token 不合法")
	}
	return n, offset, nil
}

// Token 是从 offset 开始的下一页的 page_token。
func Token(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("o:" + strconv.Itoa(offset)))
}
