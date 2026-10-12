package writer

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

// 替代关系（入库管道详设第 7 节效力信息，评估报告 K3“已废止制度被当作现行依据”）：新文档在元数据里声明它替代的文档
// （doc:<doc_id> 来自源系统字段，title:<规范化标题> 来自正文“《…》同时废止”），这里在同一数据源里关联：
// 被替代的文档记下 superseded_by，效力状态改为已废止（替代它的文档还待生效时不改）。只改文档级字段，不重新向量化。
// 声明存进 idx.supersessions，被替代的文档晚到也能反向关联；被替代的文档重新入库时这两项保留，不被抽取结果覆盖。

// titleSuffix 与 processing 的 metadata._TITLE_SUFFIX 相同：标题末尾的版本、年份括注
var titleSuffix = regexp.MustCompile(`[（(][^）)]*[）)]\s*$`)

// TitleKey 是规范化标题：去掉末尾的版本、年份括注与首尾空白（与 processing 的 title_key 一致）。
func TitleKey(title string) string {
	return strings.TrimSpace(titleSuffix.ReplaceAllString(strings.TrimSpace(title), ""))
}

const maxSupersedes = 50

func applySupersessions(ctx context.Context, tx pgx.Tx, r *ingestionv1.WriteDocumentRequest) error {
	src, doc := r.GetSourceId(), r.GetDocId()
	var targets []string
	for _, t := range r.GetMetadata().GetSupersedes() {
		if (strings.HasPrefix(t, "doc:") || strings.HasPrefix(t, "title:")) && len(t) <= 512 && t != "doc:"+doc &&
			!slices.Contains(targets, t) && len(targets) < maxSupersedes {
			targets = append(targets, t)
		}
	}
	// 新文档的声明整份替换：重新入库时去掉了的声明不再生效（已经关联的旧文档保留，替代关系不因新版改稿而撤回）
	if _, err := tx.Exec(ctx, `DELETE FROM idx.supersessions WHERE source_id = $1 AND by_doc_id = $2`, src, doc); err != nil {
		return err
	}
	if len(targets) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO idx.supersessions (source_id, by_doc_id, target) SELECT $1, $2, t FROM unnest($3::text[]) t`,
			src, doc, targets); err != nil {
			return err
		}
	}
	repealed := int16(commonv1.ValidityStatus_VALIDITY_STATUS_REPEALED)
	pending := int16(commonv1.ValidityStatus_VALIDITY_STATUS_PENDING)
	// 正向：这份文档替代的旧文档
	if _, err := tx.Exec(ctx, `
		UPDATE idx.documents old SET superseded_by = $2, updated_at = now(),
			validity_status = CASE WHEN n.validity_status = $4 THEN old.validity_status ELSE $3 END,
			metadata_missing = CASE WHEN n.validity_status = $4 THEN old.metadata_missing
				ELSE array_remove(old.metadata_missing, 'validity_status') END
		FROM idx.documents n
		WHERE n.doc_id = $2 AND old.source_id = $1 AND old.doc_id <> $2
			AND (('doc:' || old.doc_id) = ANY($5::text[]) OR (old.title_key <> '' AND ('title:' || old.title_key) = ANY($5::text[])))`,
		src, doc, repealed, pending, targets); err != nil {
		return err
	}
	// 反向：这份文档早已被别的文档声明替代（替代它的文档先入库），或它重新入库时抽取结果覆盖了原来的替代关系
	_, err := tx.Exec(ctx, `
		UPDATE idx.documents d SET superseded_by = b.doc_id,
			validity_status = CASE WHEN b.validity_status = $4 THEN d.validity_status ELSE $3 END,
			metadata_missing = CASE WHEN b.validity_status = $4 THEN d.metadata_missing
				ELSE array_remove(d.metadata_missing, 'validity_status') END
		FROM idx.supersessions s JOIN idx.documents b ON b.doc_id = s.by_doc_id
		WHERE d.doc_id = $2 AND s.source_id = $1 AND s.by_doc_id <> $2
			AND (s.target = 'doc:' || d.doc_id OR (d.title_key <> '' AND s.target = 'title:' || d.title_key))`,
		src, doc, repealed, pending)
	return err
}
