// Package push 实现 PushService（推送入库，入库管道详设 14.5）：业务系统推来的文档写成文档包并发出 DocumentChanged
// （detection = push），之后的流程与连接器拉取的相同。
//
// 鉴权在 gateway（数据源的推送登记、ACL 里的用户与群组存在、密级不低于数据源默认）；这里只核对管理 Principal 的角色
// source_pusher:<数据源> 与请求里的数据源一致，并拒绝已撤销或暂停的数据源。
//
// 已推送的状态与连接器同步共用 bf:connector:<数据源>:state（doc_id → 内容摘要+元数据摘要|权限摘要）：内容与权限都没变时
// 不发事件（重复推送不重复处理），只有权限变了走快速通道。推送源不配同步命令，状态只由推送写。
package push

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

const (
	pusherRole = "source_pusher:"
	maxBatch   = 100
	maxContent = 10 << 20
)

var (
	docIDPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,127}$`)
	tokenPattern  = regexp.MustCompile(`^(user|group):[^\s:]{1,128}$`)
	sourcePattern = regexp.MustCompile(`^src_[A-Za-z0-9_]{1,64}$`)
	// 推送的类型 → 引用里的来源类型与原文对象的扩展名（与 internal/fileshare 的格式表一致）
	formats = map[string]struct{ sourceType, ext string }{
		"text/markdown":   {"wiki", ""},
		"application/pdf": {"file", ".pdf"},
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document": {"file", ".docx"},
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":       {"table", ".xlsx"},
		"text/csv":       {"table", ".csv"},
		"message/rfc822": {"mail", ".eml"},
		"image/png":      {"image", ".png"},
		"image/jpeg":     {"image", ".jpg"},
	}
	levels = map[commonv1.Classification]string{
		commonv1.Classification_CLASSIFICATION_PUBLIC: "public", commonv1.Classification_CLASSIFICATION_INTERNAL: "internal",
		commonv1.Classification_CLASSIFICATION_CONFIDENTIAL: "confidential", commonv1.Classification_CLASSIFICATION_RESTRICTED: "restricted",
	}
	errInvalid = status.Error(codes.InvalidArgument, "文档不合法")
)

// Publisher 写文档包并发事件（publish.Publisher，Detection 为 push）。
type Publisher interface {
	PublishWithID(ctx context.Context, eventID string, d *publish.Document, change ingestionv1.ChangeType, detectedAt time.Time) error
	DeleteWithID(ctx context.Context, eventID, docID string, detectedAt time.Time) error
}

// State 是已推送的状态。
type State interface {
	Get(ctx context.Context, sourceID, docID string) (string, bool, error)
	Set(ctx context.Context, sourceID, docID, fingerprint string) error
	Delete(ctx context.Context, sourceID, docID string) error
	Count(ctx context.Context, sourceID string) (int64, error)
}

// SourceStatus 是数据源的撤销与暂停状态（internal/syncstatus）。
type SourceStatus interface {
	Revoked(ctx context.Context, sourceID string) (bool, error)
	Paused(ctx context.Context, sourceID string) (bool, error)
}

type Server struct {
	ingestionv1.UnimplementedPushServiceServer
	Publisher func(sourceID string) Publisher
	State     State
	Status    SourceStatus
	Log       *slog.Logger
	Now       func() time.Time
}

// check 核对角色与数据源状态。
func (s *Server) check(ctx context.Context, sourceID string) error {
	if !sourcePattern.MatchString(sourceID) {
		return status.Error(codes.InvalidArgument, "source_id 不合法")
	}
	if !grpcmw.HasRole(grpcmw.PrincipalFrom(ctx), pusherRole+sourceID) {
		return status.Error(codes.PermissionDenied, "不是这个数据源的推送方")
	}
	revoked, err := s.Status.Revoked(ctx, sourceID)
	if err != nil {
		return s.unavailable(ctx, err)
	}
	paused, err := s.Status.Paused(ctx, sourceID)
	if err != nil {
		return s.unavailable(ctx, err)
	}
	if revoked || paused {
		// 撤销的源不能再写入；暂停常常是为了让这个源停下，推送一样拒绝
		return grpcmw.Reason(codes.FailedPrecondition, "conflict", "数据源已撤销或暂停")
	}
	return nil
}

func (s *Server) unavailable(ctx context.Context, err error) error {
	s.Log.ErrorContext(ctx, "推送入库失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}

func (s *Server) PutDocument(ctx context.Context, r *ingestionv1.PutDocumentRequest) (*ingestionv1.PutDocumentResponse, error) {
	if err := s.check(ctx, r.GetSourceId()); err != nil {
		return nil, err
	}
	id, dup, err := s.put(ctx, r.GetSourceId(), r.GetDocument())
	if err != nil {
		return nil, err
	}
	return &ingestionv1.PutDocumentResponse{EventId: id, Duplicate: dup}, nil
}

func (s *Server) DeleteDocument(ctx context.Context, r *ingestionv1.DeleteDocumentRequest) (*ingestionv1.DeleteDocumentResponse, error) {
	if err := s.check(ctx, r.GetSourceId()); err != nil {
		return nil, err
	}
	id, err := s.delete(ctx, r.GetSourceId(), r.GetDocId())
	if err != nil {
		return nil, err
	}
	return &ingestionv1.DeleteDocumentResponse{EventId: id}, nil
}

// BatchDocuments 逐份处理；单批删除超过已推送文档的 5% 时告警（显式删除不受删除保护阈值限制，入库管道详设 14.5）。
func (s *Server) BatchDocuments(ctx context.Context, r *ingestionv1.BatchDocumentsRequest) (*ingestionv1.BatchDocumentsResponse, error) {
	if err := s.check(ctx, r.GetSourceId()); err != nil {
		return nil, err
	}
	if len(r.GetItems()) == 0 || len(r.GetItems()) > maxBatch {
		return nil, status.Error(codes.InvalidArgument, "每批 1–100 份")
	}
	total, err := s.State.Count(ctx, r.GetSourceId())
	if err != nil {
		return nil, s.unavailable(ctx, err)
	}
	out := &ingestionv1.BatchDocumentsResponse{}
	deletes := 0
	for _, it := range r.GetItems() {
		res := &ingestionv1.BatchItemResult{}
		var err error
		switch op := it.GetOp().(type) {
		case *ingestionv1.BatchItem_Upsert:
			res.DocId = op.Upsert.GetDocId()
			res.EventId, res.Duplicate, err = s.put(ctx, r.GetSourceId(), op.Upsert)
		case *ingestionv1.BatchItem_Delete:
			res.DocId = op.Delete
			res.EventId, err = s.delete(ctx, r.GetSourceId(), op.Delete)
			if err == nil {
				deletes++
			}
		default:
			err = errInvalid
		}
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				// 存储不可用时后面的也会失败：整批报错，由推送方重试（已处理的重推是幂等的）
				return nil, err
			}
			res.ErrorType, res.ErrorTitle = errorType(err), status.Convert(err).Message()
		}
		out.Results = append(out.Results, res)
	}
	if deletes > 0 && int64(deletes)*20 > total {
		s.Log.WarnContext(ctx, "单批删除超过已推送文档的 5%", slog.String("source_id", r.GetSourceId()), slog.Int("deletes", deletes),
			slog.Int64("total", total))
	}
	return out, nil
}

func errorType(err error) string {
	switch status.Code(err) {
	case codes.NotFound:
		return "not_found"
	case codes.InvalidArgument:
		return "invalid_request"
	default:
		return "service_unavailable"
	}
}

// put 写一份文档，返回事件 ID 与是否重复。
func (s *Server) put(ctx context.Context, sourceID string, d *ingestionv1.PushedDocument) (string, bool, error) {
	f, ok := formats[d.GetMimeType()]
	tokens := slices.Clone(d.GetAclTokens())
	slices.Sort(tokens)
	tokens = slices.Compact(tokens)
	level, okLevel := levels[d.GetClassification()]
	if !ok || !okLevel || !docIDPattern.MatchString(d.GetDocId()) || strings.TrimSpace(d.GetTitle()) == "" || len(tokens) == 0 ||
		slices.ContainsFunc(tokens, func(t string) bool { return !tokenPattern.MatchString(t) }) ||
		(d.GetMimeType() == "text/markdown") != (d.GetText() != "") || (d.GetText() == "") == (len(d.GetRaw()) == 0) ||
		len(d.GetText()) > maxContent || len(d.GetRaw()) > maxContent || d.GetSourceUpdatedAt() == nil {
		return "", false, errInvalid
	}
	content := []byte(d.GetText())
	if len(d.GetRaw()) > 0 {
		content = d.GetRaw()
	}
	contentHash := digest(content)
	aclFP := digest([]byte(strings.Join(tokens, "\n") + "\n" + level))
	// 标题、类型与地址变了也要重新入库（引用里显示的是它们），与内容一起算进“内容”部分
	metaFP := digest([]byte(d.GetTitle() + "\x00" + d.GetMimeType() + "\x00" + d.GetSourceUri()))
	fp := contentHash + "+" + metaFP + "|" + aclFP
	old, found, err := s.State.Get(ctx, sourceID, d.GetDocId())
	if err != nil {
		return "", false, s.unavailable(ctx, err)
	}
	change := ingestionv1.ChangeType_CHANGE_TYPE_UPSERT
	switch {
	case found && old == fp:
		return "", true, nil
	case found && contentPart(old) == contentPart(fp):
		change = ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY
	}
	doc := &publish.Document{
		Meta: docmeta.Meta{DocID: d.GetDocId(), Title: strings.TrimSpace(d.GetTitle()), Classification: level, ACLTokens: tokens,
			UpdatedAt: d.GetSourceUpdatedAt().AsTime(), ACLFingerprint: aclFP},
		SourceURI: d.GetSourceUri(), SourceType: f.sourceType, ContentType: d.GetMimeType(), ContentHash: contentHash,
	}
	if d.GetText() != "" {
		doc.Text = d.GetText()
	} else {
		doc.Raw, doc.FileName = d.GetRaw(), d.GetDocId()+f.ext
	}
	id := publish.NewEventID()
	if err := s.Publisher(sourceID).PublishWithID(ctx, id, doc, change, s.Now()); err != nil {
		return "", false, s.unavailable(ctx, err)
	}
	// 事件发出后才记状态：中途失败时推送方重推，会再发一次（下游按内容摘要幂等）
	if err := s.State.Set(ctx, sourceID, d.GetDocId(), fp); err != nil {
		return "", false, s.unavailable(ctx, err)
	}
	return id, false, nil
}

func (s *Server) delete(ctx context.Context, sourceID, docID string) (string, error) {
	if !docIDPattern.MatchString(docID) {
		return "", errInvalid
	}
	_, found, err := s.State.Get(ctx, sourceID, docID)
	if err != nil {
		return "", s.unavailable(ctx, err)
	}
	if !found {
		return "", status.Error(codes.NotFound, "没有推送过这份文档")
	}
	id := publish.NewEventID()
	if err := s.Publisher(sourceID).DeleteWithID(ctx, id, docID, s.Now()); err != nil {
		return "", s.unavailable(ctx, err)
	}
	if err := s.State.Delete(ctx, sourceID, docID); err != nil {
		return "", s.unavailable(ctx, err)
	}
	return id, nil
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func contentPart(fp string) string {
	c, _, _ := strings.Cut(fp, "|")
	return c
}
