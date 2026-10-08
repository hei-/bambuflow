package grpcmw

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reasonDomain 是业务错误码的命名空间。
const reasonDomain = "bambuflow"

// Reason 返回带业务错误码的 gRPC 错误（如 self_approval_forbidden）。gateway 把登记过的错误码原样转成对外的
// problem type（governance 详设 7.2）；没登记的按 gRPC 状态码转，不会把内部细节带出去。
func Reason(code codes.Code, reason, msg string) error {
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: reasonDomain})
	if err != nil {
		return status.Error(code, msg)
	}
	return st.Err()
}

// ReasonOf 取出 Reason 附带的业务错误码；没有时为空。
func ReasonOf(err error) string {
	var se interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &se) {
		return ""
	}
	for _, d := range se.GRPCStatus().Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == reasonDomain {
			return info.GetReason()
		}
	}
	return ""
}
