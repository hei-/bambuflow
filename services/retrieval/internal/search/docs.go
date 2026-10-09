package search

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
)

// 列举与计数的上限（K2 详设第 3 节）：计数超过 10,000 只报“不少于”，列表最多翻到第 10,000 条。
const (
	maxDocCount     = 10000
	defaultPageSize = 20
	maxPageSize     = 50
)

// CountDocuments 统计调用者可见、符合条件的文档数。权限、密级、删除标记、拒绝名单与 Search 共用同一份必含条件，
// 在计数之前生效：无权文档不进入计数（K2 详设第 8 节）。
func (s *Server) CountDocuments(ctx context.Context, r *retrievalv1.CountDocumentsRequest) (*retrievalv1.CountDocumentsResponse, error) {
	if err := validPurpose(ctx); err != nil {
		return nil, err
	}
	done, err := s.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	f, pv, version, err := s.docScope(ctx, r.GetFilters())
	if err != nil {
		return nil, err
	}
	n, exact, err := s.count(ctx, version, f)
	if err != nil {
		return nil, err
	}
	resp := &retrievalv1.CountDocumentsResponse{TotalCount: n, TotalIsExact: exact, IndexVersion: version, PolicyVersion: pv}
	if by := groupBy[r.GetGroupBy()]; by != "" && len(f.Sources) > 0 {
		groups, err := s.Store.CountGroups(ctx, version, f, by, maxGroups)
		if err != nil {
			s.Log.ErrorContext(ctx, "分组计数失败", slog.Any("err", err))
			return nil, errUnavailable
		}
		for _, g := range groups {
			resp.Groups = append(resp.Groups, &retrievalv1.CountGroup{Value: groupValue(by, g.Value), Count: g.Count})
		}
	}
	if resp.MetadataGaps, err = s.gaps(ctx, version, f); err != nil {
		return nil, err
	}
	return resp, nil
}

// ListDocuments 按标题分页列出调用者可见、符合条件的文档，并给出与 CountDocuments 同口径的总数。
func (s *Server) ListDocuments(ctx context.Context, r *retrievalv1.ListDocumentsRequest) (*retrievalv1.ListDocumentsResponse, error) {
	if err := validPurpose(ctx); err != nil {
		return nil, err
	}
	size := int(r.GetPageSize())
	switch {
	case size == 0:
		size = defaultPageSize
	case size < 0 || size > maxPageSize:
		return nil, status.Errorf(codes.InvalidArgument, "page_size 需要 1–%d", maxPageSize)
	}
	done, err := s.admit(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	f, pv, version, err := s.docScope(ctx, r.GetFilters())
	if err != nil {
		return nil, err
	}
	offset, err := decodePageToken(r.GetPageToken(), version)
	if err != nil {
		return nil, err
	}
	n, exact, err := s.count(ctx, version, f)
	if err != nil {
		return nil, err
	}
	size = min(size, maxDocCount-offset)
	byDate := r.GetOrderBy() == retrievalv1.DocumentOrder_DOCUMENT_ORDER_EFFECTIVE_DATE_DESC
	docs, err := s.Store.ListDocs(ctx, version, f, byDate, offset, size)
	if err != nil {
		s.Log.ErrorContext(ctx, "列举文档失败", slog.Any("err", err))
		return nil, errUnavailable
	}
	resp := &retrievalv1.ListDocumentsResponse{TotalCount: n, TotalIsExact: exact, IndexVersion: version, PolicyVersion: pv}
	for _, d := range docs {
		resp.Documents = append(resp.Documents, &retrievalv1.DocumentSummary{
			DocId: d.DocID, DocVersion: d.DocVersion, Title: d.Title, SourceId: d.SourceID,
			SourceUri: d.SourceURI, SourceType: d.SourceType, Classification: d.Classification, Metadata: d.Metadata,
			SummaryChunkId: d.SummaryChunkID,
		})
	}
	if resp.MetadataGaps, err = s.gaps(ctx, version, f); err != nil {
		return nil, err
	}
	if next := offset + len(docs); len(docs) == size && next < maxDocCount && int64(next) < n {
		resp.NextPageToken = encodePageToken(version, next)
	}
	return resp, nil
}

// docScope 鉴权并取线上索引版本。
func (s *Server) docScope(ctx context.Context, filters *commonv1.Filters) (Filter, string, string, error) {
	if err := ValidateMeta(filters); err != nil {
		return Filter{}, "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	f, pv, err := s.authorize(ctx)
	if err != nil {
		return Filter{}, "", "", err
	}
	// 调用方的条件只能收窄授权范围（与 Search 相同）
	f = f.Narrow(filters)
	version, _, err := s.indexVersion(ctx)
	if errors.Is(err, ErrNoLiveIndex) {
		return Filter{}, "", "", status.Error(codes.Unavailable, "还没有线上索引版本")
	}
	if _, ok := status.FromError(err); ok && err != nil {
		return Filter{}, "", "", err
	}
	if err != nil {
		s.Log.ErrorContext(ctx, "读取线上索引版本失败", slog.Any("err", err))
		return Filter{}, "", "", errUnavailable
	}
	return f, pv, version, nil
}

const maxGroups = 50

var groupBy = map[retrievalv1.CountGroupBy]string{
	retrievalv1.CountGroupBy_COUNT_GROUP_BY_DOC_TYPE:        "doc_type",
	retrievalv1.CountGroupBy_COUNT_GROUP_BY_DEPT:            "dept",
	retrievalv1.CountGroupBy_COUNT_GROUP_BY_VALIDITY_STATUS: "validity_status",
	retrievalv1.CountGroupBy_COUNT_GROUP_BY_YEAR:            "year",
}

// groupValue 把效力状态的编号换成契约里的名字（如 VALIDITY_STATUS_EFFECTIVE），未知为空。
func groupValue(by, v string) string {
	if by != "validity_status" {
		return v
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n == 0 {
		return ""
	}
	return commonv1.ValidityStatus(int32(n)).String()
}

// gaps 统计每个被筛选字段的缺口：其他条件都符合、但这个字段为空的可见文档数（K2 详设第 4 节）。
// 权限等必含条件照常生效，缺口里不含无权文档。
func (s *Server) gaps(ctx context.Context, version string, f Filter) ([]*retrievalv1.MetadataGap, error) {
	var out []*retrievalv1.MetadataGap
	for _, field := range f.Meta.Fields() {
		g := f
		g.Meta = f.Meta.Without(field)
		g.Meta.Missing = field
		n, _, err := s.count(ctx, version, g)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			out = append(out, &retrievalv1.MetadataGap{Field: field, Count: n})
		}
	}
	return out, nil
}

func (s *Server) count(ctx context.Context, version string, f Filter) (int64, bool, error) {
	if len(f.Sources) == 0 {
		return 0, true, nil
	}
	n, err := s.Store.CountDocs(ctx, version, f, maxDocCount+1)
	if err != nil {
		s.Log.ErrorContext(ctx, "统计文档失败", slog.Any("err", err))
		return 0, false, errUnavailable
	}
	if n > maxDocCount {
		return maxDocCount, false, nil
	}
	return n, true, nil
}

// 翻页令牌带上索引版本：换了版本后偏移量对不上，宁可让调用方重新列，也不返回重复或遗漏的条目。
func encodePageToken(version string, offset int) string {
	return base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%s|%d", version, offset))
}

func decodePageToken(token, version string) (int, error) {
	if token == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	v, off, ok := strings.Cut(string(b), "|")
	n, nerr := strconv.Atoi(off)
	if err != nil || !ok || nerr != nil || n < 0 || n >= maxDocCount {
		return 0, status.Error(codes.InvalidArgument, "page_token 不合法")
	}
	if v != version {
		return 0, status.Error(codes.FailedPrecondition, "索引版本已变化，请从第一页重新列举")
	}
	return n, nil
}
