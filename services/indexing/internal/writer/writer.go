package writer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
	"github.com/bambu-ai/bambuflow/libs/textseg"
)

type Server struct {
	ingestionv1.UnimplementedIndexWriterServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
}

func (s *Server) WriteDocument(ctx context.Context, r *ingestionv1.WriteDocumentRequest) (*ingestionv1.WriteDocumentResponse, error) {
	if err := validate(r); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var resp *ingestionv1.WriteDocumentResponse
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		resp, err = write(ctx, tx, r)
		return err
	})
	if err != nil {
		s.Log.ErrorContext(ctx, "写入文档失败", slog.String("doc_id", r.GetDocId()), slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	return resp, nil
}

func write(ctx context.Context, tx pgx.Tx, r *ingestionv1.WriteDocumentRequest) (*ingestionv1.WriteDocumentResponse, error) {
	version, ok, err := pendingVersion(ctx, tx, r.GetEmbeddingModelVersion(), r.GetChunkerVersion())
	if err != nil {
		return nil, err
	}
	if !ok {
		return &ingestionv1.WriteDocumentResponse{Result: ingestionv1.WriteResult_WRITE_RESULT_MODEL_MISMATCH, IndexVersion: version}, nil
	}
	occurred := r.GetOccurredAt().AsTime()

	var docVersion int64
	var hash string
	var prevOccurred time.Time
	err = tx.QueryRow(ctx, `SELECT doc_version, content_hash, occurred_at FROM idx.documents WHERE doc_id = $1 FOR UPDATE`,
		r.GetDocId()).Scan(&docVersion, &hash, &prevOccurred)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if exists && occurred.Before(prevOccurred) {
		// 乱序到达的旧事件：已入库的内容更新，不能被它覆盖
		return &ingestionv1.WriteDocumentResponse{Result: ingestionv1.WriteResult_WRITE_RESULT_STALE_VERSION, DocVersion: docVersion, IndexVersion: version}, nil
	}
	// 删除优先：删除标记晚于这次内容的产生时间，丢弃这次写入；只有源端重新创建（更晚的产生时间）才能再入库
	var tombstonedAt *time.Time
	if err := tx.QueryRow(ctx, `SELECT tombstoned_at FROM idx.doc_acl WHERE doc_id = $1 AND state = 'tombstoned'`,
		r.GetDocId()).Scan(&tombstonedAt); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if tombstonedAt != nil && !occurred.After(*tombstonedAt) {
		return &ingestionv1.WriteDocumentResponse{Result: ingestionv1.WriteResult_WRITE_RESULT_STALE_VERSION, DocVersion: docVersion, IndexVersion: version}, nil
	}
	result := ingestionv1.WriteResult_WRITE_RESULT_UNCHANGED
	written := int32(0)
	// 内容没变但已有的块是别的嵌入模型或分块器产生的（换过模型）：必须重写，
	// 否则待发布版本会引用旧模型的向量，查询向量与它们不在同一空间，检索不到
	stale := false
	if exists && hash == r.GetContentHash() {
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM idx.chunks WHERE doc_id = $1 AND doc_version = $2
				AND (embedding_model <> $3 OR chunker_version <> $4))`,
			r.GetDocId(), docVersion, r.GetEmbeddingModelVersion(), r.GetChunkerVersion()).Scan(&stale); err != nil {
			return nil, err
		}
		// 已入库的版本还没有这份摘要块（摘要上线前入库的文档，或上次增强失败）：同样重写一个内容版本补上。
		// 摘要的生成是确定的，内容不变时摘要块 ID 不变，重复入库不会因此产生新版本
		if s := r.GetEnrichment().GetSummaryChunk(); !stale && s != nil {
			var has bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM idx.chunks WHERE doc_id = $1 AND doc_version = $2 AND chunk_id = $3 AND kind = 'summary')`,
				r.GetDocId(), docVersion, s.GetChunkId()).Scan(&has); err != nil {
				return nil, err
			}
			stale = !has
		}
	}
	if !exists || hash != r.GetContentHash() || stale {
		docVersion++
		result = ingestionv1.WriteResult_WRITE_RESULT_WRITTEN
	}
	md := metadataOf(r.GetMetadata())
	// 内容没变的重复写入也更新元数据：词表变更后重跑抽取就能补齐历史文档，不必重新向量化
	if _, err := tx.Exec(ctx, `
		INSERT INTO idx.documents (doc_id, source_id, doc_version, content_hash, title, source_uri, source_type, owner, occurred_at, updated_at,
			doc_type, dept, topic_codes, effective_date, validity_status, metadata_missing, vocabulary_version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (doc_id) DO UPDATE SET source_id = EXCLUDED.source_id, doc_version = EXCLUDED.doc_version,
			content_hash = EXCLUDED.content_hash, title = EXCLUDED.title, source_uri = EXCLUDED.source_uri,
			source_type = EXCLUDED.source_type, owner = EXCLUDED.owner, occurred_at = EXCLUDED.occurred_at, updated_at = now(),
			doc_type = EXCLUDED.doc_type, dept = EXCLUDED.dept, topic_codes = EXCLUDED.topic_codes,
			effective_date = EXCLUDED.effective_date, validity_status = EXCLUDED.validity_status,
			metadata_missing = EXCLUDED.metadata_missing, vocabulary_version = EXCLUDED.vocabulary_version`,
		r.GetDocId(), r.GetSourceId(), docVersion, r.GetContentHash(), r.GetTitle(), r.GetSourceUri(),
		int32(r.GetSourceType()), r.GetOwner(), occurred,
		md.docType, md.dept, md.topics, md.effectiveDate, md.status, md.missing, md.vocabVersion); err != nil {
		return nil, err
	}
	if result == ingestionv1.WriteResult_WRITE_RESULT_WRITTEN {
		for _, c := range r.GetChunks() {
			added, err := insertChunk(ctx, tx, r, docVersion, c, "content", strings.Join(c.GetHeadingPath(), " "))
			if err != nil {
				return nil, err
			}
			if added {
				written++
			}
		}
		if err := writeEnrichment(ctx, tx, r, docVersion); err != nil {
			return nil, err
		}
	}
	// 权限与密级永远取最新值；acl_seq 单调递增，供对账与撤权位点使用
	if _, err := tx.Exec(ctx, `
		INSERT INTO idx.doc_acl (doc_id, source_id, acl_tokens, classification, state, acl_seq, acl_occurred_at, updated_at)
		VALUES ($1, $2, $3, $4, 'active', nextval('idx.acl_seq'), $5, now())
		ON CONFLICT (doc_id) DO UPDATE SET
			-- 快速通道记录过更晚的权限变化时保留它：乱序到达的内容写入不能把权限改回旧值
			acl_tokens = CASE WHEN idx.doc_acl.acl_occurred_at > EXCLUDED.acl_occurred_at
				THEN idx.doc_acl.acl_tokens ELSE EXCLUDED.acl_tokens END,
			classification = CASE WHEN idx.doc_acl.acl_occurred_at > EXCLUDED.acl_occurred_at
				THEN idx.doc_acl.classification ELSE EXCLUDED.classification END,
			acl_occurred_at = GREATEST(idx.doc_acl.acl_occurred_at, EXCLUDED.acl_occurred_at),
			state = 'active', tombstoned_at = NULL, acl_seq = nextval('idx.acl_seq'), updated_at = now()`,
		r.GetDocId(), r.GetSourceId(), nonNil(r.GetAclTokens()), int32(r.GetClassification()), occurred); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO idx.version_docs (version, doc_id, doc_version) VALUES ($1, $2, $3)
		ON CONFLICT (version, doc_id) DO UPDATE SET doc_version = EXCLUDED.doc_version`,
		version, r.GetDocId(), docVersion); err != nil {
		return nil, err
	}
	// 关系挂在这个内容版本上，随版本清单进入待发布版本；内容没变时也替换，抽取器升级后重跑即可补齐
	if err := writeRelations(ctx, tx, r.GetDocId(), docVersion, r.GetRelations()); err != nil {
		return nil, err
	}
	return &ingestionv1.WriteDocumentResponse{Result: result, DocVersion: docVersion, WrittenChunks: written, IndexVersion: version}, nil
}

// insertChunk 写一块，返回是否新写入（已存在时为 false）。second 是关键词索引里权重 B 的文字：
// 正文块是标题路径，摘要块是文档关键词。
func insertChunk(ctx context.Context, tx pgx.Tx, r *ingestionv1.WriteDocumentRequest, docVersion int64,
	c *ingestionv1.Chunk, kind, second string) (bool, error) {
	span, err := spanJSON(c.GetSpan())
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO idx.chunks (chunk_id, doc_version, doc_id, parent_chunk_id, heading_path, text, text_hash, span, embedding, embedding_model, chunker_version, kind, injection_flag, tsv)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			setweight(to_tsvector('simple', $14), 'A') || setweight(to_tsvector('simple', $15), 'B') ||
			setweight(to_tsvector('simple', $16), 'C'))
		ON CONFLICT DO NOTHING`,
		c.GetChunkId(), docVersion, r.GetDocId(), c.GetParentChunkId(),
		// 第一个标题之前的正文没有标题路径：空切片要写成空数组，不能是 NULL
		nonNil(c.GetHeadingPath()), c.GetText(), c.GetTextHash(),
		span, pgvector.NewVector(c.GetEmbedding()), r.GetEmbeddingModelVersion(), r.GetChunkerVersion(), kind, c.GetInjectionFlag(),
		words(r.GetTitle()), words(second), words(c.GetText()))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// writeEnrichment 写这个内容版本的摘要块与章节摘要，并把文档的 summary_chunk_id、关键词指向它们。
// 没有增强结果（未生成或失败）时清空：文档级字段永远对应最新的内容版本，不能留着上一版的摘要。
func writeEnrichment(ctx context.Context, tx pgx.Tx, r *ingestionv1.WriteDocumentRequest, docVersion int64) error {
	e := r.GetEnrichment()
	summaryID := ""
	if s := e.GetSummaryChunk(); s != nil {
		if _, err := insertChunk(ctx, tx, r, docVersion, s, "summary", strings.Join(e.GetKeywords(), " ")); err != nil {
			return err
		}
		summaryID = s.GetChunkId()
	}
	for _, sec := range e.GetSections() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idx.sections (doc_id, doc_version, parent_chunk_id, heading_path, summary, keywords)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
			r.GetDocId(), docVersion, sec.GetParentChunkId(), nonNil(sec.GetHeadingPath()), sec.GetSummary(),
			nonNil(sec.GetKeywords())); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `UPDATE idx.documents SET summary_chunk_id = $2, keywords = $3 WHERE doc_id = $1`,
		r.GetDocId(), summaryID, nonNil(e.GetKeywords()))
	return err
}

// TombstoneDocument 写文档级删除标记：对线上版本立即生效（retrieval 只检索 state = active 的文档），
// 回滚也不会让它复活。删除时间早于已入库内容的产生时间时视为过期的删除，不生效。
func (s *Server) TombstoneDocument(ctx context.Context, r *ingestionv1.TombstoneDocumentRequest) (*ingestionv1.TombstoneDocumentResponse, error) {
	if r.GetSourceId() == "" || r.GetDocId() == "" || r.GetOccurredAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "source_id、doc_id、occurred_at 必填")
	}
	var result ingestionv1.TombstoneResult
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		result, err = tombstone(ctx, tx, r)
		return err
	})
	if err != nil {
		s.Log.ErrorContext(ctx, "写删除标记失败", slog.String("doc_id", r.GetDocId()), slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	return &ingestionv1.TombstoneDocumentResponse{Result: result}, nil
}

func tombstone(ctx context.Context, tx pgx.Tx, r *ingestionv1.TombstoneDocumentRequest) (ingestionv1.TombstoneResult, error) {
	at := r.GetOccurredAt().AsTime()
	var state string
	var contentAt time.Time
	err := tx.QueryRow(ctx, `
		SELECT a.state, d.occurred_at FROM idx.doc_acl a JOIN idx.documents d USING (doc_id)
		WHERE a.doc_id = $1 AND a.source_id = $2 FOR UPDATE OF a`, r.GetDocId(), r.GetSourceId()).Scan(&state, &contentAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ingestionv1.TombstoneResult_TOMBSTONE_RESULT_NOT_FOUND, nil
	case err != nil:
		return 0, err
	case state == "tombstoned":
		return ingestionv1.TombstoneResult_TOMBSTONE_RESULT_UNCHANGED, nil
	case at.Before(contentAt):
		return ingestionv1.TombstoneResult_TOMBSTONE_RESULT_STALE, nil
	}
	_, err = tx.Exec(ctx, `
		UPDATE idx.doc_acl SET state = 'tombstoned', tombstoned_at = $2, acl_seq = nextval('idx.acl_seq'), updated_at = now()
		WHERE doc_id = $1`, r.GetDocId(), at)
	return ingestionv1.TombstoneResult_TOMBSTONE_RESULT_TOMBSTONED, err
}

// UpdateAcl 是权限快速通道：只改文档级权限与密级，不重算向量、不进待发布版本，对线上立即生效。
func (s *Server) UpdateAcl(ctx context.Context, r *ingestionv1.UpdateAclRequest) (*ingestionv1.UpdateAclResponse, error) {
	if r.GetSourceId() == "" || r.GetDocId() == "" || r.GetOccurredAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "source_id、doc_id、occurred_at 必填")
	}
	if r.GetClassification() == commonv1.Classification_CLASSIFICATION_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "密级必填")
	}
	var result ingestionv1.AclResult
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		result, err = updateACL(ctx, tx, r)
		return err
	})
	if err != nil {
		s.Log.ErrorContext(ctx, "更新权限失败", slog.String("doc_id", r.GetDocId()), slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	return &ingestionv1.UpdateAclResponse{Result: result}, nil
}

func updateACL(ctx context.Context, tx pgx.Tx, r *ingestionv1.UpdateAclRequest) (ingestionv1.AclResult, error) {
	at := r.GetOccurredAt().AsTime()
	var tokens []string
	var classification int32
	var last *time.Time
	err := tx.QueryRow(ctx, `
		SELECT acl_tokens, classification, acl_occurred_at FROM idx.doc_acl
		WHERE doc_id = $1 AND source_id = $2 FOR UPDATE`, r.GetDocId(), r.GetSourceId()).Scan(&tokens, &classification, &last)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ingestionv1.AclResult_ACL_RESULT_NOT_FOUND, nil
	case err != nil:
		return 0, err
	case last != nil && at.Before(*last):
		return ingestionv1.AclResult_ACL_RESULT_STALE, nil
	}
	newTokens := nonNil(r.GetAclTokens())
	if slices.Equal(tokens, newTokens) && classification == int32(r.GetClassification()) {
		_, err := tx.Exec(ctx, `UPDATE idx.doc_acl SET acl_occurred_at = $2 WHERE doc_id = $1`, r.GetDocId(), at)
		return ingestionv1.AclResult_ACL_RESULT_UNCHANGED, err
	}
	_, err = tx.Exec(ctx, `
		UPDATE idx.doc_acl SET acl_tokens = $2, classification = $3, acl_occurred_at = $4,
			acl_seq = nextval('idx.acl_seq'), updated_at = now()
		WHERE doc_id = $1`, r.GetDocId(), newTokens, int32(r.GetClassification()), at)
	return ingestionv1.AclResult_ACL_RESULT_UPDATED, err
}

// pendingVersion 返回待发布版本，没有时新建。待发布版本的嵌入模型或分块器与请求不一致时 ok 为 false。
func pendingVersion(ctx context.Context, tx pgx.Tx, model, chunker string) (string, bool, error) {
	var version, curModel, curChunker string
	err := tx.QueryRow(ctx, `SELECT version, embedding_model, chunker_version FROM idx.index_versions WHERE status = 'pending' FOR UPDATE`).
		Scan(&version, &curModel, &curChunker)
	if err == nil {
		return version, curModel == model && curChunker == chunker, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return "", false, err
	}
	version = "ix-" + time.Now().UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b)
	// 并发时两个事务可能同时新建：唯一索引只允许一个待发布版本，另一个会失败并由调用方重试
	_, err = tx.Exec(ctx, `INSERT INTO idx.index_versions (version, status, embedding_model, chunker_version) VALUES ($1, 'pending', $2, $3)`,
		version, model, chunker)
	return version, err == nil, err
}

// words 把文本切成以空格分隔的词，交给 to_tsvector('simple', …)；查询侧用同一个分词器（libs/textseg）。
func words(text string) string {
	return strings.Join(textseg.Tokens(text), " ")
}

// spanJSON 是 chunks.span 列的格式；没有的字段写 null，与 retrieval 的读取一致。
func spanJSON(s *commonv1.Span) ([]byte, error) {
	var bbox []float32
	if b := s.GetBbox(); b != nil {
		bbox = []float32{b.GetX0(), b.GetY0(), b.GetX1(), b.GetY1()}
	}
	return json.Marshal(map[string]any{
		"char_start": s.GetCharStart(), "char_end": s.GetCharEnd(),
		"line_start": s.LineStart, "line_end": s.LineEnd, "page": s.Page, "bbox": bbox,
	})
}

func nonNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

// Publish 把待发布版本切换为线上版本：prod 别名原子地指向它，原线上版本标为 retired。
// 没有待发布版本、或版本里没有文档时拒绝。评测门禁随 P2-T6 接入。
//
// 增量发布：原线上版本里本轮没有写到、且未打删除标记的文档原样带入，返回带入的文档数。
// 删除通过删除标记显式表达，所以只同步了部分文档也不会让其余文档从线上消失。
// 只在嵌入模型与分块器都相同时带入：换模型后旧向量与新向量不在同一空间，必须全量重建。
func Publish(ctx context.Context, pool *pgxpool.Pool) (string, int64, error) {
	var version string
	var carried int64
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT version FROM idx.index_versions WHERE status = 'pending' FOR UPDATE`).Scan(&version); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoPending
			}
			return err
		}
		var docs int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM idx.version_docs WHERE version = $1`, version).Scan(&docs); err != nil {
			return err
		}
		if docs == 0 {
			return errors.New("待发布版本里没有文档，拒绝发布")
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO idx.version_docs (version, doc_id, doc_version)
			SELECT $1, vd.doc_id, vd.doc_version
			FROM idx.aliases a
			JOIN idx.index_versions lv ON lv.version = a.version
			JOIN idx.index_versions pv ON pv.version = $1
			JOIN idx.version_docs vd ON vd.version = a.version
			JOIN idx.doc_acl acl ON acl.doc_id = vd.doc_id
			WHERE a.alias = 'prod' AND acl.state = 'active'
			  AND lv.embedding_model = pv.embedding_model AND lv.chunker_version = pv.chunker_version
			  -- 块级再核对一次：旧版本里混进的其他模型的块不能被带入
			  AND NOT EXISTS (SELECT 1 FROM idx.chunks c WHERE c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
			      AND (c.embedding_model <> pv.embedding_model OR c.chunker_version <> pv.chunker_version))
			ON CONFLICT (version, doc_id) DO NOTHING`, version)
		if err != nil {
			return err
		}
		carried = tag.RowsAffected()
		if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'retired' WHERE status = 'live'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'live', published_at = now() WHERE version = $1`, version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO idx.aliases (alias, version, switched_at) VALUES ('prod', $1, now())
			ON CONFLICT (alias) DO UPDATE SET version = EXCLUDED.version, switched_at = now()`, version); err != nil {
			return err
		}
		// 新版本上线通知（技术设计文档 13.7）与别名切换同一事务：上线了就一定有事件。
		// 索引由各租户共用，事件不带租户，发给所有订阅了它的租户
		evt, pb, err := outbox.New(outbox.IndexVersionLive, "", version, time.Now())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO idx.outbox (event_id, payload) VALUES ($1, $2)`, evt.GetEventId(), pb)
		return err
	})
	return version, carried, err
}

// 发件表的转发语句（outbox.Relay）。
const (
	OutboxSelectSQL = `SELECT event_id, payload FROM idx.outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`
	OutboxMarkSQL   = `UPDATE idx.outbox SET sent_at = now() WHERE event_id = ANY($1)`
)

// ErrNoPending 表示没有待发布版本：增量同步本轮没有任何变更时属于正常情况。
var ErrNoPending = errors.New("没有待发布版本")
