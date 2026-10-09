package writer

import (
	"context"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

var termCode = regexp.MustCompile(`^[A-Z][A-Z0-9-]{1,63}$`)

// 词表 → 改编码的语句。主题是数组：替换后去重，保持编码唯一
var remapSQL = map[string]string{
	"doc_type": `UPDATE idx.documents SET doc_type = $2, updated_at = now() WHERE doc_type = $1`,
	"dept":     `UPDATE idx.documents SET dept = $2, updated_at = now() WHERE dept = $1`,
	"topic": `UPDATE idx.documents SET topic_codes = ARRAY(SELECT DISTINCT unnest(array_replace(topic_codes, $1, $2)) ORDER BY 1),
		updated_at = now() WHERE $1 = ANY(topic_codes)`,
}

func (a *Admin) RemapTerms(ctx context.Context, r *adminv1.RemapTermsRequest) (*adminv1.RemapTermsResponse, error) {
	for _, m := range r.GetRemaps() {
		if remapSQL[m.GetVocabulary()] == "" || !termCode.MatchString(m.GetFromCode()) || !termCode.MatchString(m.GetToCode()) ||
			m.GetFromCode() == m.GetToCode() {
			return nil, status.Error(codes.InvalidArgument, "vocabulary 或编码不合法")
		}
	}
	var n int64
	err := pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		for _, m := range r.GetRemaps() {
			tag, err := tx.Exec(ctx, remapSQL[m.GetVocabulary()], m.GetFromCode(), m.GetToCode())
			if err != nil {
				return err
			}
			n += tag.RowsAffected()
		}
		return nil
	})
	if err != nil {
		a.Log.ErrorContext(ctx, "重新标注受控词表编码失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	if n > 0 {
		a.Log.InfoContext(ctx, "已按替代编码重新标注文档", slog.Int64("documents", n), slog.Int("remaps", len(r.GetRemaps())))
	}
	return &adminv1.RemapTermsResponse{Documents: n}, nil
}
