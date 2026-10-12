package api

import (
	"context"
	"net/http"
	"strconv"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// 紧急拒绝规则（技术设计文档 13.2，governance 详设 7.2）：GET/POST /v1/admin/emergency-denials，由 governance 实现。

var denialScopes = map[string]adminv1.DenialScope{
	"source":   adminv1.DenialScope_DENIAL_SCOPE_SOURCE,
	"document": adminv1.DenialScope_DENIAL_SCOPE_DOCUMENT,
	"tenant":   adminv1.DenialScope_DENIAL_SCOPE_TENANT,
}

type denialJSON struct {
	DenialID  string `json:"denial_id"`
	Scope     string `json:"scope"`
	Target    string `json:"target"`
	Reason    string `json:"reason"`
	CreatedBy string `json:"created_by"`
	CreatedAt string `json:"created_at"`
	// pending：已登记、还没写进索引库的拒绝名单（governance 持续重试）；active：检索已生效
	Status    string `json:"status"`
	AppliedAt string `json:"applied_at,omitempty"`
}

func denialOf(d *adminv1.EmergencyDenial) denialJSON {
	out := denialJSON{DenialID: d.GetDenialId(), Scope: enumName(d.GetScope()), Target: d.GetTarget(), Reason: d.GetReason(),
		CreatedBy: d.GetCreatedBy(), CreatedAt: rfc3339(d.GetCreatedAt()), Status: enumName(d.GetStatus())}
	if d.GetAppliedAt() != nil {
		out.AppliedAt = rfc3339(d.GetAppliedAt())
	}
	return out
}

type denialListJSON struct {
	Denials       []denialJSON `json:"denials"`
	NextPageToken string       `json:"next_page_token,omitempty"`
}

func (s *Server) listDenials(ctx context.Context, in *adminInput) (adminOutput, error) {
	size, err := pageSize(in)
	if err != nil {
		return adminOutput{}, err
	}
	resp, err := s.Denials.ListEmergencyDenials(ctx, &adminv1.ListEmergencyDenialsRequest{PageSize: size, PageToken: in.query.Get("page_token")})
	if err != nil {
		return adminOutput{}, err
	}
	out := denialListJSON{Denials: []denialJSON{}, NextPageToken: resp.GetNextPageToken()}
	for _, d := range resp.GetDenials() {
		out.Denials = append(out.Denials, denialOf(d))
	}
	return adminOutput{body: out}, nil
}

type createDenialJSON struct {
	Scope  string `json:"scope"`
	Target string `json:"target"`
	Reason string `json:"reason"`
}

func (s *Server) createDenial(ctx context.Context, in *adminInput) (adminOutput, error) {
	var req createDenialJSON
	if err := decodeBody(in.body, &req); err != nil {
		return adminOutput{}, err
	}
	scope, ok := denialScopes[req.Scope]
	if !ok {
		return adminOutput{}, problemInvalid
	}
	resp, err := s.Denials.CreateEmergencyDenial(ctx, &adminv1.CreateEmergencyDenialRequest{Scope: scope, Target: req.Target, Reason: req.Reason})
	if err != nil {
		return adminOutput{}, err
	}
	// 同一对象已有规则时返回已有的那一条（200），新登记的为 201
	code := http.StatusOK
	if resp.GetCreated() {
		code = http.StatusCreated
	}
	return adminOutput{status: code, body: denialOf(resp.GetDenial())}, nil
}

// pageSize 读查询参数 page_size（1–200，不传为 0，由实现方取默认值）。
func pageSize(in *adminInput) (int32, error) {
	v := in.query.Get("page_size")
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 || n > 200 {
		return 0, problemInvalid
	}
	return int32(n), nil
}
