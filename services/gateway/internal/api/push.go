package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/gateway/internal/admission"
	"github.com/hei-/bambuflow/services/gateway/internal/audit"
	"github.com/hei-/bambuflow/services/gateway/internal/authn"
	"github.com/hei-/bambuflow/services/gateway/internal/issuer"
)

// 推送入库（入库管道详设 14.5，部署配置清单 P15）：没有现成连接器的业务系统把文档推给平台，由 connectors 写成文档包并发出
// DocumentChanged，之后的流程与连接器拉取的相同。
//
// 鉴权都在这里：只接受服务身份（客户端凭证），且须是该数据源注册时登记、随数据源审批的推送客户端（配置快照里的 push_sources）；
// ACL 必填，其中的用户与群组须在平台身份库里存在；密级不低于数据源的默认密级。之后以管理 Principal（角色 source_pusher:<数据源>）
// 调 connectors，它再核对一次数据源与角色一致。不涉及租户：文档属于数据源，哪些租户能检索由数据源授权决定。

const (
	// 单份文档的原文上限（解码后）；更大的文件等大文件上传引用（待定）
	maxPushContent = 10 << 20
	// base64 膨胀约 4/3，再留出元数据
	maxPushBody      = 14 << 20
	maxPushBatchBody = 32 << 20
	// 每批最多 100 份（入库管道详设 14.5 建议值）
	maxPushBatchItems = 100
	// ACL 条目上限：源系统的 ACL 一般在几十项以内，过大的多半是把整个部门逐人展开了，应改用群组
	maxACLEntries = 1000
	pushDeadline  = 30 * time.Second
	pusherRole    = "source_pusher:"
)

var (
	pushDocIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)
	aclNamePattern   = regexp.MustCompile(`^[^\s:]{1,128}$`)
	// processing 能解析的类型（services/ingestion/processing 的 parsers.py）；text/markdown 的正文放 content，其余放 content_base64
	pushMimeTypes = []string{"text/markdown", "application/pdf",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"text/csv", "message/rfc822", "image/png", "image/jpeg"}
	classificationOrder = map[string]commonv1.Classification{
		"public": commonv1.Classification_CLASSIFICATION_PUBLIC, "internal": commonv1.Classification_CLASSIFICATION_INTERNAL,
		"confidential": commonv1.Classification_CLASSIFICATION_CONFIDENTIAL, "restricted": commonv1.Classification_CLASSIFICATION_RESTRICTED,
	}
	problemServiceRequired = problem{http.StatusForbidden, "service_identity_required", "推送入库只接受服务身份（客户端凭证）的令牌"}
)

type pushACLJSON struct {
	Users  []string `json:"users"`
	Groups []string `json:"groups"`
}

type pushDocumentJSON struct {
	Title    string `json:"title"`
	MimeType string `json:"mime_type"`
	// text/markdown 的正文
	Content string `json:"content,omitempty"`
	// 其他类型的原文
	ContentBase64   string       `json:"content_base64,omitempty"`
	ACL             *pushACLJSON `json:"acl"`
	Classification  string       `json:"classification,omitempty"`
	SourceUpdatedAt string       `json:"source_updated_at"`
	SourceURI       string       `json:"source_uri,omitempty"`
}

type pushResultJSON struct {
	DocID      string `json:"doc_id"`
	EventID    string `json:"event_id,omitempty"`
	Duplicate  bool   `json:"duplicate,omitempty"`
	ErrorType  string `json:"error_type,omitempty"`
	ErrorTitle string `json:"error_title,omitempty"`
}

// authorizePush 认证推送客户端并核对数据源的推送登记，返回调 connectors 用的管理 Principal 与推送登记。
// 先认证再读请求体：未认证的调用方不能让 gateway 读上几十 MB。
func (s *Server) authorizePush(r *http.Request, c *call) (string, *configv1.PushSource, error) {
	sourceID := r.PathValue("source_id")
	c.admin = &audit.Admin{Owner: "connectors", Method: r.Method, Path: r.URL.Path}
	client, err := s.Verifier.VerifyService(c.ctx, r.Header.Get("Authorization"))
	switch {
	case errors.Is(err, authn.ErrServiceIdentityRequired):
		return "", nil, problemServiceRequired
	case err != nil:
		s.Log.InfoContext(c.ctx, "平台令牌校验失败", slog.Any("err", err))
		return "", nil, problemUnauth
	}
	c.actor = client
	// 按数据源限流（入库管道详设 14.5），桶的大小与按客户端的相同
	if !s.Limits.Allow(c.ctx, s.ClientRule, "push:"+sourceID) {
		return "", nil, problemRateLimited
	}
	done, ok := s.Overload.Admit(admission.PriorityBatch)
	if !ok {
		return "", nil, problemOverloaded
	}
	c.release = append(c.release, done)
	ps, ok := s.PushSources(sourceID)
	if !ok || ps.GetPushClient() != client {
		// 不接受推送、不存在与不是登记的客户端一样返回 403，不让调用方探测有哪些数据源
		return "", nil, problemForbidden
	}
	token, jti, err := s.Issuer.IssueOperator(issuer.Subject{Username: "service:" + client, Actor: client,
		Roles: []string{pusherRole + sourceID}})
	if err != nil {
		return "", nil, fmt.Errorf("签发 Principal: %w", err)
	}
	c.jti = jti
	return token, ps, nil
}

// readPushBody 读请求体并把摘要记进审计（请求体含原文，审计只记摘要）。
func readPushBody(r *http.Request, c *call) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, problemTooLarge
		}
		return nil, problemInvalid
	}
	sum := sha256.Sum256(body)
	c.admin.RequestSHA = hex.EncodeToString(sum[:])
	return body, nil
}

// pushDocument 校验并转换一份推送的文档；不合法时返回带原因的 400。
func (s *Server) pushDocument(c *call, ps *configv1.PushSource, docID string, d *pushDocumentJSON) (*ingestionv1.PushedDocument, error) {
	invalid := func(why string) error { return problem{http.StatusBadRequest, "invalid_request", why} }
	if !pushDocIDPattern.MatchString(docID) {
		return nil, invalid("doc_id 只能由小写字母、数字和连字符组成，最长 128 个字符")
	}
	if d == nil {
		return nil, invalid("缺少文档")
	}
	title := strings.TrimSpace(d.Title)
	if title == "" || utf8.RuneCountInString(title) > 500 || !slices.Contains(pushMimeTypes, d.MimeType) ||
		len(d.SourceURI) > 2048 {
		return nil, invalid("title（最多 500 字）、mime_type 或 source_uri 不合法")
	}
	out := &ingestionv1.PushedDocument{DocId: docID, Title: title, MimeType: d.MimeType, SourceUri: d.SourceURI}
	if d.MimeType == "text/markdown" {
		if d.Content == "" || d.ContentBase64 != "" || len(d.Content) > maxPushContent || !utf8.ValidString(d.Content) {
			return nil, invalid("text/markdown 的正文放在 content（UTF-8，最大 10 MB）")
		}
		out.Text = d.Content
	} else {
		raw, err := base64.StdEncoding.DecodeString(d.ContentBase64)
		if d.Content != "" || err != nil || len(raw) == 0 || len(raw) > maxPushContent {
			return nil, invalid("二进制原文放在 content_base64（标准 base64，解码后最大 10 MB）")
		}
		out.Raw = raw
	}
	updated, err := time.Parse(time.RFC3339, d.SourceUpdatedAt)
	if err != nil || updated.After(s.Now().Add(5*time.Minute)) {
		return nil, invalid("source_updated_at 必填（RFC 3339），不能晚于当前时间")
	}
	out.SourceUpdatedAt = timestamppb.New(updated)
	out.Classification = ps.GetDefaultClassification()
	if d.Classification != "" {
		cls, ok := classificationOrder[d.Classification]
		if !ok || cls < ps.GetDefaultClassification() {
			return nil, invalid("classification 不合法，或低于数据源的默认密级 " + enumName(ps.GetDefaultClassification()))
		}
		out.Classification = cls
	}
	// ACL 必填（入库管道详设 14.5）：没有 ACL 的文档入库后谁能看就说不清了
	if d.ACL == nil || len(d.ACL.Users)+len(d.ACL.Groups) == 0 || len(d.ACL.Users)+len(d.ACL.Groups) > maxACLEntries {
		return nil, invalid("acl 必填：users 与 groups 至少一项，合计最多 1000 项")
	}
	for _, n := range append(slices.Clone(d.ACL.Users), d.ACL.Groups...) {
		if !aclNamePattern.MatchString(n) {
			return nil, invalid(fmt.Sprintf("acl 里的 %q 不合法", n))
		}
	}
	users, groups := dedupe(d.ACL.Users), dedupe(d.ACL.Groups)
	badUsers, badGroups, err := s.Identities.Unknown(c.ctx, users, groups)
	if err != nil {
		return nil, fmt.Errorf("读取平台身份库: %w", err)
	}
	if len(badUsers)+len(badGroups) > 0 {
		return nil, invalid("acl 里有平台身份库中不存在的用户或群组：" + strings.Join(head(append(prefixed("user:", badUsers),
			prefixed("group:", badGroups)...), 10), "、"))
	}
	out.AclTokens = append(prefixed("user:", users), prefixed("group:", groups)...)
	return out, nil
}

func dedupe(xs []string) []string {
	out := slices.Clone(xs)
	slices.Sort(out)
	return slices.Compact(out)
}

func prefixed(p string, xs []string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, p+x)
	}
	return out
}

func head(xs []string, n int) []string { return xs[:min(len(xs), n)] }

// ---- PUT /v1/sources/{source_id}/documents/{doc_id} ----

func (s *Server) putDocument(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, ps, err := s.authorizePush(r, c)
	if err != nil {
		return nil, err
	}
	body, err := readPushBody(r, c)
	if err != nil {
		return nil, err
	}
	var d pushDocumentJSON
	if err := decodeBody(body, &d); err != nil {
		return nil, err
	}
	doc, err := s.pushDocument(c, ps, r.PathValue("doc_id"), &d)
	if err != nil {
		return nil, err
	}
	ctx, cancel := downstream(c.ctx, principal, "push", "", pushDeadline)
	defer cancel()
	resp, err := s.Push.PutDocument(ctx, &ingestionv1.PutDocumentRequest{SourceId: ps.GetSourceId(), Document: doc})
	if err != nil {
		return nil, fromGRPC(err)
	}
	c.httpStatus = http.StatusAccepted
	return pushResultJSON{DocID: doc.GetDocId(), EventID: resp.GetEventId(), Duplicate: resp.GetDuplicate()}, nil
}

// ---- DELETE /v1/sources/{source_id}/documents/{doc_id} ----

func (s *Server) deletePushedDocument(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, ps, err := s.authorizePush(r, c)
	if err != nil {
		return nil, err
	}
	docID := r.PathValue("doc_id")
	if !pushDocIDPattern.MatchString(docID) {
		return nil, problemNotFound
	}
	ctx, cancel := downstream(c.ctx, principal, "push", "", pushDeadline)
	defer cancel()
	resp, err := s.Push.DeleteDocument(ctx, &ingestionv1.DeleteDocumentRequest{SourceId: ps.GetSourceId(), DocId: docID})
	if err != nil {
		return nil, fromGRPC(err)
	}
	c.httpStatus = http.StatusAccepted
	return pushResultJSON{DocID: docID, EventID: resp.GetEventId()}, nil
}

// ---- POST /v1/sources/{source_id}/documents/batch ----

type batchItemJSON struct {
	// upsert 或 delete
	Op       string            `json:"op"`
	DocID    string            `json:"doc_id"`
	Document *pushDocumentJSON `json:"document,omitempty"`
}

// batchPush 逐份校验：不合法的那一份直接给出错误结果，其余的交给 connectors，结果按请求顺序合并。
func (s *Server) batchPush(_ http.ResponseWriter, r *http.Request, c *call) (any, error) {
	principal, ps, err := s.authorizePush(r, c)
	if err != nil {
		return nil, err
	}
	body, err := readPushBody(r, c)
	if err != nil {
		return nil, err
	}
	var req struct {
		Items []batchItemJSON `json:"items"`
	}
	if err := decodeBody(body, &req); err != nil {
		return nil, err
	}
	if len(req.Items) == 0 || len(req.Items) > maxPushBatchItems {
		return nil, problem{http.StatusBadRequest, "invalid_request", "items 须有 1–100 份"}
	}
	results := make([]pushResultJSON, len(req.Items))
	var items []*ingestionv1.BatchItem
	var at []int
	for i, it := range req.Items {
		results[i].DocID = it.DocID
		var item *ingestionv1.BatchItem
		switch it.Op {
		case "upsert":
			doc, err := s.pushDocument(c, ps, it.DocID, it.Document)
			var p problem
			if errors.As(err, &p) {
				results[i].ErrorType, results[i].ErrorTitle = p.typ, p.title
				continue
			}
			if err != nil {
				return nil, err
			}
			item = &ingestionv1.BatchItem{Op: &ingestionv1.BatchItem_Upsert{Upsert: doc}}
		case "delete":
			if !pushDocIDPattern.MatchString(it.DocID) || it.Document != nil {
				results[i].ErrorType, results[i].ErrorTitle = problemInvalid.typ, "delete 只带合法的 doc_id"
				continue
			}
			item = &ingestionv1.BatchItem{Op: &ingestionv1.BatchItem_Delete{Delete: it.DocID}}
		default:
			results[i].ErrorType, results[i].ErrorTitle = problemInvalid.typ, "op 只能是 upsert 或 delete"
			continue
		}
		items, at = append(items, item), append(at, i)
	}
	if len(items) > 0 {
		ctx, cancel := downstream(c.ctx, principal, "push", "", pushDeadline)
		defer cancel()
		resp, err := s.Push.BatchDocuments(ctx, &ingestionv1.BatchDocumentsRequest{SourceId: ps.GetSourceId(), Items: items})
		if err != nil {
			return nil, fromGRPC(err)
		}
		if len(resp.GetResults()) != len(items) {
			return nil, errors.New("connectors 返回的结果数与请求不符")
		}
		for j, x := range resp.GetResults() {
			results[at[j]] = pushResultJSON{DocID: x.GetDocId(), EventID: x.GetEventId(), Duplicate: x.GetDuplicate(),
				ErrorType: x.GetErrorType(), ErrorTitle: x.GetErrorTitle()}
		}
	}
	return struct {
		Results []pushResultJSON `json:"results"`
	}{results}, nil
}
