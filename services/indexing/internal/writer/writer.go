package writer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/outbox"
	revocations "github.com/hei-/bambuflow/libs/platform-go/revocation"
	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
	"github.com/hei-/bambuflow/libs/textseg"
	"github.com/hei-/bambuflow/services/indexing/internal/fence"
)

type Server struct {
	ingestionv1.UnimplementedIndexWriterServiceServer
	Pool *pgxpool.Pool
	Log  *slog.Logger
	// Notify 在文档权限变化或写删除标记提交后发出 ChunkAclChanged（retrieval 据此清检索结果缓存）；为 nil 时不发。
	// 尽力而为：发不出去只记日志，retrieval 组装时还会按最新权限复核，缓存最多多留到过期
	Notify func(ctx context.Context, docIDs []string, change string) error
	// Fence 是读副本下撤权的登记、抬位点与等待（internal/fence）；为 nil 时不做（测试）
	Fence *fence.Fence
}

// revoke 在主库事务外按详设 4.2 的顺序做一次权限或删除变更（fence.Revoke）。
func (s *Server) revoke(ctx context.Context, members []string, txn func(pgx.Tx) error, notify func()) error {
	return s.Fence.Revoke(ctx, members, func() error { return pgx.BeginFunc(ctx, s.Pool, txn) }, notify)
}

func (s *Server) notify(ctx context.Context, docID, change string) {
	if s.Notify == nil {
		return
	}
	if err := s.Notify(ctx, []string{docID}, change); err != nil {
		s.Log.WarnContext(ctx, "发出 ChunkAclChanged 失败，检索结果缓存到期后自然失效", slog.String("doc_id", docID), slog.Any("err", err))
	}
}

// lagPause 是副本回放落后时每次内容写入前的等待
const lagPause = 500 * time.Millisecond

var throttled, _ = telemetry.Meter("bambuflow/indexing").Int64Counter("indexing.write.throttled",
	metric.WithDescription("因权限读副本回放落后而放慢的内容写入次数"))

func (s *Server) WriteDocument(ctx context.Context, r *ingestionv1.WriteDocumentRequest) (*ingestionv1.WriteDocumentResponse, error) {
	if err := validate(r); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// 权限读副本回放延迟超过 1 秒时每次写入先等一会儿，压低写入速度，主要压住全量重建（元数据库可用性详设 7.1）；
	// 这个延迟只用于限速，不参与正确性判断
	if s.Fence.Lag() > 1 {
		throttled.Add(ctx, 1)
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, "已取消")
		case <-time.After(lagPause):
		}
	}
	var resp *ingestionv1.WriteDocumentResponse
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		resp, err = write(ctx, tx, r)
		return err
	})
	if err == nil {
		// 内容写入也会改文档级权限（可能收窄）：抬撤权位点，不等副本
		err = s.Fence.Commit(ctx, false)
	}
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

	var docVersion, since int64
	var hash string
	var prevOccurred time.Time
	err = tx.QueryRow(ctx, `SELECT doc_version, content_since, content_hash, occurred_at FROM idx.documents WHERE doc_id = $1 FOR UPDATE`,
		r.GetDocId()).Scan(&docVersion, &since, &hash, &prevOccurred)
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
	// target 是这次写入在待发布版本里引用的内容版本。内容没变时复用当前内容里块都是这个嵌入模型与分块器的那个版本；
	// 没有（换过模型，或全量重建期间另一个模型先写了）就另起一个，否则待发布版本会引用另一个模型的向量，检索不到
	target := int64(0)
	if exists && hash == r.GetContentHash() {
		if target, err = reusableVersion(ctx, tx, r, since); err != nil {
			return nil, err
		}
	}
	if target == 0 {
		docVersion++
		target = docVersion
		if !exists || hash != r.GetContentHash() {
			since = docVersion
		}
		result = ingestionv1.WriteResult_WRITE_RESULT_WRITTEN
	}
	md := metadataOf(r.GetMetadata())
	// 内容没变的重复写入也更新元数据：词表变更后重跑抽取就能补齐历史文档，不必重新向量化
	if _, err := tx.Exec(ctx, `
		INSERT INTO idx.documents (doc_id, source_id, doc_version, content_hash, title, source_uri, source_type, owner, occurred_at, updated_at,
			doc_type, dept, topic_codes, effective_date, validity_status, metadata_missing, vocabulary_version, content_since, title_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT (doc_id) DO UPDATE SET source_id = EXCLUDED.source_id, doc_version = EXCLUDED.doc_version,
			content_since = EXCLUDED.content_since,
			content_hash = EXCLUDED.content_hash, title = EXCLUDED.title, source_uri = EXCLUDED.source_uri,
			source_type = EXCLUDED.source_type, owner = EXCLUDED.owner, occurred_at = EXCLUDED.occurred_at, updated_at = now(),
			doc_type = EXCLUDED.doc_type, dept = EXCLUDED.dept, topic_codes = EXCLUDED.topic_codes,
			effective_date = EXCLUDED.effective_date, validity_status = EXCLUDED.validity_status,
			metadata_missing = EXCLUDED.metadata_missing, vocabulary_version = EXCLUDED.vocabulary_version,
			title_key = EXCLUDED.title_key`,
		r.GetDocId(), r.GetSourceId(), docVersion, r.GetContentHash(), r.GetTitle(), r.GetSourceUri(),
		int32(r.GetSourceType()), r.GetOwner(), occurred,
		md.docType, md.dept, md.topics, md.effectiveDate, md.status, md.missing, md.vocabVersion, since, TitleKey(r.GetTitle())); err != nil {
		return nil, err
	}
	if err := applySupersessions(ctx, tx, r); err != nil {
		return nil, err
	}
	if result == ingestionv1.WriteResult_WRITE_RESULT_WRITTEN {
		for _, c := range r.GetChunks() {
			added, err := insertChunk(ctx, tx, r, target, c, "content", strings.Join(c.GetHeadingPath(), " "))
			if err != nil {
				return nil, err
			}
			if added {
				written++
			}
		}
		if err := writeEnrichment(ctx, tx, r, target); err != nil {
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
		version, r.GetDocId(), target); err != nil {
		return nil, err
	}
	// 关系挂在这个内容版本上，随版本清单进入待发布版本；内容没变时也替换，抽取器升级后重跑即可补齐
	if err := writeRelations(ctx, tx, r.GetDocId(), target, r.GetRelations()); err != nil {
		return nil, err
	}
	return &ingestionv1.WriteDocumentResponse{Result: result, DocVersion: target, WrittenChunks: written, IndexVersion: version}, nil
}

// reusableVersion 在当前内容的各个内容版本（不早于 since）里找块全是这次的嵌入模型与分块器的那一个，没有时返回 0。
// 已入库的版本还没有这份摘要块（摘要上线前入库的文档，或上次增强失败）时同样不复用，重写一个内容版本补上；
// 摘要的生成是确定的，内容不变时摘要块 ID 不变，重复入库不会因此产生新版本。
func reusableVersion(ctx context.Context, tx pgx.Tx, r *ingestionv1.WriteDocumentRequest, since int64) (int64, error) {
	var v int64
	err := tx.QueryRow(ctx, `
		SELECT doc_version FROM idx.chunks WHERE doc_id = $1 AND doc_version >= $2
		GROUP BY doc_version HAVING bool_and(embedding_model = $3 AND chunker_version = $4)
		ORDER BY doc_version DESC LIMIT 1`,
		r.GetDocId(), since, r.GetEmbeddingModelVersion(), r.GetChunkerVersion()).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if s := r.GetEnrichment().GetSummaryChunk(); s != nil {
		var has bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM idx.chunks WHERE doc_id = $1 AND doc_version = $2 AND chunk_id = $3 AND kind = 'summary')`,
			r.GetDocId(), v, s.GetChunkId()).Scan(&has); err != nil {
			return 0, err
		}
		if !has {
			return 0, nil
		}
	}
	return v, nil
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
		INSERT INTO idx.chunks (chunk_id, doc_version, doc_id, parent_chunk_id, heading_path, text, text_hash, span, embedding, embedding_model, chunker_version, kind, injection_flag, tsv,
			pii_ciphertext, pii_wrapped_key, pii_key_id, input_hash, sparse)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			setweight(to_tsvector('simple', $14), 'A') || setweight(to_tsvector('simple', $15), 'B') ||
			setweight(to_tsvector('simple', $16), 'C'), $17, NULLIF($18, ''), NULLIF($19, ''), $20, $21)
		ON CONFLICT DO NOTHING`,
		c.GetChunkId(), docVersion, r.GetDocId(), c.GetParentChunkId(),
		// 第一个标题之前的正文没有标题路径：空切片要写成空数组，不能是 NULL
		nonNil(c.GetHeadingPath()), c.GetText(), c.GetTextHash(),
		span, pgvector.NewVector(c.GetEmbedding()), r.GetEmbeddingModelVersion(), r.GetChunkerVersion(), kind, c.GetInjectionFlag(),
		words(r.GetTitle()), words(second), words(c.GetText()),
		c.GetPii().GetCiphertext(), c.GetPii().GetWrappedKey(), c.GetPii().GetKeyId(), c.GetInputHash(), sparseOf(c.GetSparse()))
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
	err := s.revoke(ctx, []string{revocations.DocMember(r.GetDocId())}, func(tx pgx.Tx) error {
		var err error
		result, err = tombstone(ctx, tx, r)
		return err
	}, func() {
		if result == ingestionv1.TombstoneResult_TOMBSTONE_RESULT_TOMBSTONED {
			s.notify(ctx, r.GetDocId(), "tombstone")
		}
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
	err := s.revoke(ctx, []string{revocations.DocMember(r.GetDocId())}, func(tx pgx.Tx) error {
		var err error
		result, err = updateACL(ctx, tx, r)
		return err
	}, func() {
		if result == ingestionv1.AclResult_ACL_RESULT_UPDATED {
			s.notify(ctx, r.GetDocId(), "acl")
		}
	})
	if err != nil {
		s.Log.ErrorContext(ctx, "更新权限失败", slog.String("doc_id", r.GetDocId()), slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	return &ingestionv1.UpdateAclResponse{Result: result}, nil
}

// maxLookup 是一次向量缓存查询的输入摘要上限
const maxLookup = 1000

var inputHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// LookupVectors 按向量化输入的摘要与嵌入模型取已入库的向量（入库管道详设第 8 节）。同一输入在多个版本里都有时取任意一个：
// 输入与模型都相同，向量就相同。
func (s *Server) LookupVectors(ctx context.Context, r *ingestionv1.LookupVectorsRequest) (*ingestionv1.LookupVectorsResponse, error) {
	hashes := r.GetInputHashes()
	if r.GetEmbeddingModelVersion() == "" || len(hashes) > maxLookup ||
		slices.ContainsFunc(hashes, func(h string) bool { return !inputHashPattern.MatchString(h) }) {
		return nil, status.Error(codes.InvalidArgument, "embedding_model_version 必填，input_hashes 最多 1000 个 SHA-256")
	}
	out := &ingestionv1.LookupVectorsResponse{}
	if len(hashes) == 0 {
		return out, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT ON (input_hash) input_hash, embedding, sparse FROM idx.chunks
		WHERE embedding_model = $1 AND input_hash = ANY($2::text[])
		ORDER BY input_hash, sparse IS NULL`, r.GetEmbeddingModelVersion(), hashes)
	if err != nil {
		s.Log.ErrorContext(ctx, "查询向量缓存失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "向量缓存不可用")
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		var v pgvector.Vector
		var sp *pgvector.SparseVector
		if err := rows.Scan(&h, &v, &sp); err != nil {
			return nil, status.Error(codes.Unavailable, "向量缓存不可用")
		}
		cv := &ingestionv1.CachedVector{InputHash: h, Embedding: v.Slice()}
		if sp != nil {
			cv.Sparse = map[uint32]float32{}
			for i, idx := range sp.Indices() {
				// sparsevec 的下标从 0 起、小于维数，不会是负数；读出异常的值就跳过这一项
				if idx < 0 || idx >= SparseDim {
					continue
				}
				cv.Sparse[uint32(idx)] = sp.Values()[i]
			}
		}
		out.Vectors = append(out.Vectors, cv)
	}
	if rows.Err() != nil {
		return nil, status.Error(codes.Unavailable, "向量缓存不可用")
	}
	return out, nil
}

// SparseDim 是稀疏向量的维数（bge-m3 的词表大小，与 idx.chunks.sparse 的列定义一致）
const SparseDim = 250002

// sparseOf 把块的稀疏向量转成 sparsevec；没有或词元 ID 越界时写 NULL（这块不参与稀疏召回）。
func sparseOf(m map[uint32]float32) any {
	if len(m) == 0 {
		return nil
	}
	el := make(map[int32]float32, len(m))
	for k, v := range m {
		if k >= SparseDim {
			return nil
		}
		el[int32(k)] = v
	}
	v := pgvector.NewSparseVectorFromMap(el, SparseDim)
	return v
}

// granteePattern 是分区被授权方的格式（与文档 ACL 的 token 相同）
var granteePattern = regexp.MustCompile(`^(group|dept|user):[^\s:]+$`)

// SetPartitionGrants 替换一个分区的全部被授权方（总体设计 ADR-14）。只改分区授权表，不动索引与文档权限：
// retrieval 下一个请求就按新的授权展开。
func (s *Server) SetPartitionGrants(ctx context.Context, r *ingestionv1.SetPartitionGrantsRequest) (*ingestionv1.SetPartitionGrantsResponse, error) {
	grantees := slices.Clone(r.GetGrantees())
	slices.Sort(grantees)
	grantees = slices.Compact(grantees)
	if r.GetSourceId() == "" || r.GetPartition() == "" || len(r.GetPartition()) > 512 || r.GetOccurredAt() == nil ||
		slices.ContainsFunc(grantees, func(g string) bool { return !granteePattern.MatchString(g) }) {
		return nil, status.Error(codes.InvalidArgument, "source_id、partition、occurred_at 必填，被授权方须是 group:/dept:/user: 开头的 token")
	}
	at := r.GetOccurredAt().AsTime()
	var result ingestionv1.AclResult
	err := s.revoke(ctx, []string{revocations.PartitionMember(r.GetSourceId(), r.GetPartition())}, func(tx pgx.Tx) error {
		// 先占住这个分区的状态行，同一分区的并发变化串行处理
		if _, err := tx.Exec(ctx, `INSERT INTO idx.partition_grant_state (source_id, partition, occurred_at)
			VALUES ($1, $2, '-infinity') ON CONFLICT DO NOTHING`, r.GetSourceId(), r.GetPartition()); err != nil {
			return err
		}
		var last time.Time
		if err := tx.QueryRow(ctx, `SELECT occurred_at FROM idx.partition_grant_state WHERE source_id = $1 AND partition = $2 FOR UPDATE`,
			r.GetSourceId(), r.GetPartition()).Scan(&last); err != nil {
			return err
		}
		if at.Before(last) {
			result = ingestionv1.AclResult_ACL_RESULT_STALE
			return nil
		}
		rows, err := tx.Query(ctx, `SELECT grantee FROM idx.partition_grants WHERE source_id = $1 AND partition = $2 ORDER BY grantee`,
			r.GetSourceId(), r.GetPartition())
		if err != nil {
			return err
		}
		current, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		result = ingestionv1.AclResult_ACL_RESULT_UNCHANGED
		if !slices.Equal(current, grantees) {
			result = ingestionv1.AclResult_ACL_RESULT_UPDATED
			if _, err := tx.Exec(ctx, `DELETE FROM idx.partition_grants WHERE source_id = $1 AND partition = $2`,
				r.GetSourceId(), r.GetPartition()); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO idx.partition_grants (source_id, partition, grantee)
				SELECT $1, $2, g FROM unnest($3::text[]) g`, r.GetSourceId(), r.GetPartition(), grantees); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE idx.partition_grant_state SET occurred_at = $3, updated_at = now()
			WHERE source_id = $1 AND partition = $2`, r.GetSourceId(), r.GetPartition(), at)
		return err
	}, func() {})
	if err != nil {
		s.Log.ErrorContext(ctx, "更新分区授权失败", slog.String("source_id", r.GetSourceId()), slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "写入失败")
	}
	if result == ingestionv1.AclResult_ACL_RESULT_UPDATED {
		s.Log.InfoContext(ctx, "分区授权已更新", slog.String("source_id", r.GetSourceId()), slog.String("partition", r.GetPartition()),
			slog.Int("grantees", len(grantees)))
	}
	return &ingestionv1.SetPartitionGrantsResponse{Result: result}, nil
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

// pendingVersion 返回与请求的嵌入模型、分块器一致的待发布版本，没有时新建。全量重建期间有两个待发布版本
// （重建的新模型与线上模型各一个），内容变更按 processing 所用的模型分别写入两边（入库管道详设第 10 节第 3 步）。
// 只为线上的模型新建（还没有线上版本时为第一份）；其他模型的版本只能由全量重建建立，此时 ok 为 false。
func pendingVersion(ctx context.Context, tx pgx.Tx, model, chunker string) (string, bool, error) {
	var version string
	err := tx.QueryRow(ctx, `SELECT version FROM idx.index_versions
		WHERE status = 'pending' AND embedding_model = $1 AND chunker_version = $2 FOR UPDATE`, model, chunker).Scan(&version)
	if err == nil {
		return version, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	var other string
	err = tx.QueryRow(ctx, `
		SELECT version FROM idx.index_versions
		WHERE (status = 'live' OR (status = 'pending' AND NOT EXISTS (SELECT 1 FROM idx.index_versions WHERE status = 'live')))
		  AND (embedding_model <> $1 OR chunker_version <> $2)
		LIMIT 1`, model, chunker).Scan(&other)
	if err == nil {
		return other, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return "", false, err
	}
	version = "ix-" + time.Now().UTC().Format("20060102-150405") + "-" + hex.EncodeToString(b)
	// 并发时两个事务可能同时新建：唯一索引只允许每组模型一个待发布版本，另一个会失败并由调用方重试
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

// Publish 把待发布版本 version 切换为线上版本：prod 别名原子地指向它，原线上版本标为 retired。
// version 不是待发布版本、或版本里没有文档时拒绝。
//
// 增量发布：原线上版本里本轮没有写到、且未打删除标记的文档原样带入，返回带入的文档数。
// 删除通过删除标记显式表达，所以只同步了部分文档也不会让其余文档从线上消失。
// 只在嵌入模型与分块器都相同时带入：换模型后旧向量与新向量不在同一空间，必须全量重建。
// 全量重建的版本上线时，旧模型的增量待发布版本随之作废：其中的变更已同时写进重建版本。
//
// 发布写变更审计（index.publish），与切换别名在同一事务里：审计写不进去就不发布（技术设计文档 8.3）。
// Auditor 写变更审计（libs/platform-go/changeaudit）。发布、按保留期过期、物理删除这类不经 gateway 的后台操作
// 在事务提交前写，写不进去就回滚（fail-closed）。
type Auditor interface {
	Write(ctx context.Context, action, objectID string, detail any) error
}

func Publish(ctx context.Context, pool *pgxpool.Pool, version string, audit Auditor) (int64, error) {
	var carried int64
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var model, chunker string
		if err := tx.QueryRow(ctx, `SELECT embedding_model, chunker_version FROM idx.index_versions WHERE version = $1 AND status = 'pending' FOR UPDATE`,
			version).Scan(&model, &chunker); err != nil {
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
		// 全量重建的版本必须写完全部在用文档才能上线（换模型后旧文档带不进来，缺的就会从线上消失）
		if err := checkRebuildComplete(ctx, tx, version); err != nil {
			return err
		}
		// 门禁之前已带入过一次（CarryForward）；这里再带一次，补上门禁期间线上版本的变化（重复带入不改变结果）
		var err error
		if carried, err = carryForward(ctx, tx, version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'retired' WHERE status = 'live'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE idx.index_versions SET status = 'discarded'
			WHERE status = 'pending' AND (embedding_model <> $1 OR chunker_version <> $2)`, model, chunker); err != nil {
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
		if err := audit.Write(ctx, "index.publish", version, map[string]any{"docs": docs, "carried_from_live": carried,
			"embedding_model": model, "chunker_version": chunker}); err != nil {
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
	return carried, err
}

// carrySQL 把线上版本里本轮没有写到、且未打删除标记的文档带入待发布版本（增量发布，见 Publish）。
const carrySQL = `
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
			ON CONFLICT (version, doc_id) DO NOTHING`

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func carryForward(ctx context.Context, q execer, version string) (int64, error) {
	tag, err := q.Exec(ctx, carrySQL, version)
	return tag.RowsAffected(), err
}

// CarryForward 在评测门禁之前把线上版本里没变的文档带入待发布版本，门禁评测的才是上线后的完整版本；
// 否则待发布版本里只有本批写入的文档，评测会把“本批之外的文档检索不到”算成质量下降。
func CarryForward(ctx context.Context, pool *pgxpool.Pool, version string) (int64, error) {
	return carryForward(ctx, pool, version)
}

// 发件表的转发语句（outbox.Relay）。
const (
	OutboxSelectSQL = `SELECT event_id, payload FROM idx.outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`
	OutboxMarkSQL   = `UPDATE idx.outbox SET sent_at = now() WHERE event_id = ANY($1)`
)

// ErrNoPending 表示没有待发布版本：增量同步本轮没有任何变更时属于正常情况。
var ErrNoPending = errors.New("没有待发布版本")
