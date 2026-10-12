// Package store 是 retrieval 对索引（pgvector，ADR-9 的 P0 参考后端）与元数据库的只读访问。
//
// 连接以只读事务运行（default_transaction_read_only），即使凭证配置错成可写账号也写不进去。
// 专用只读账号由部署时的 Vault 数据库角色提供（部署配置清单），compose 里沿用开发账号。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
	pgxvec "github.com/pgvector/pgvector-go/pgx"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	"github.com/hei-/bambuflow/services/retrieval/internal/search"
)

// PG 实现 search.Store。
type PG struct {
	pool *pgxpool.Pool
}

// Open 建立连接池，不立即连接：元数据库暂时不可用时进程照常启动，请求按 fail-closed 拒绝，readiness 为 false。
func Open(ctx context.Context, dsn string) (*PG, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { return pgxvec.RegisterTypes(ctx, c) }
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PG{pool: pool}, nil
}

func (s *PG) Close() { s.pool.Close() }

// Ping 用于 readiness。
func (s *PG) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *PG) LiveVersion(ctx context.Context) (string, string, error) {
	var version, model string
	err := s.pool.QueryRow(ctx, `
		SELECT a.version, v.embedding_model FROM idx.aliases a JOIN idx.index_versions v USING (version)
		WHERE a.alias = 'prod'`).Scan(&version, &model)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", search.ErrNoLiveIndex
	}
	return version, model, err
}

// VersionModel 返回给定索引版本的嵌入模型；已丢弃（取消的全量重建）的版本按不存在处理。
func (s *PG) VersionModel(ctx context.Context, version string) (string, error) {
	var model string
	err := s.pool.QueryRow(ctx, `SELECT embedding_model FROM idx.index_versions WHERE version = $1 AND status <> 'discarded'`,
		version).Scan(&model)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", search.ErrNoLiveIndex
	}
	return model, err
}

// 两路召回共用的选择列与必含条件：只在版本清单列出的 (doc_id, doc_version) 里检索，四个必含条件全部写进 WHERE，
// 与排序在同一条查询里完成，不存在“先取候选再过滤”导致的漏召回或越权（retrieval 详设 5.4）。
const (
	candidateCols = `c.chunk_id, c.doc_id, c.doc_version, c.parent_chunk_id, c.heading_path, c.text, c.span,
		d.title, d.source_uri, d.source_type, d.owner, a.classification, a.source_id, c.injection_flag,
		c.pii_ciphertext, coalesce(c.pii_wrapped_key, ''), coalesce(c.pii_key_id, '')`
	candidateFrom = `idx.version_docs vd
		JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version
		JOIN idx.documents d ON d.doc_id = vd.doc_id
		JOIN idx.doc_acl a ON a.doc_id = vd.doc_id`
	// 普通召回只取正文块：摘要块单独供主题汇总使用（K2 详设第 3 节“摘要向量放在单独集合”）
	recallOnly = ` AND c.kind = 'content'`
	mandatory  = `vd.version = $1
		AND a.state = 'active'
		AND a.classification <= $2
		AND a.acl_tokens && $3::text[]
		AND a.source_id = ANY($4::text[])
		AND NOT EXISTS (SELECT 1 FROM idx.deny_list dl WHERE dl.doc_id = vd.doc_id)
		AND NOT EXISTS (SELECT 1 FROM idx.deny_scopes ds WHERE ds.kind = 'source' AND ds.value = a.source_id)`
)

// Dense 按向量距离排序。P0、P1 数据量小，按精确距离排序；数据量上来后按详设 5.4 选择近似检索策略。
func (s *PG) Dense(ctx context.Context, version string, f search.Filter, vec []float32, k int) ([]search.Candidate, error) {
	meta, metaArgs := metaWhere(f.Meta, 7)
	rows, err := s.pool.Query(ctx, `
		SELECT `+candidateCols+`, greatest(0, least(1, 1 - (c.embedding <=> $5)))::float8
		FROM `+candidateFrom+`
		WHERE `+mandatory+recallOnly+meta+`
		ORDER BY c.embedding <=> $5, c.chunk_id
		LIMIT $6`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, pgvector.NewVector(vec), k}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// Chunks 按块 ID 取块（检索结果缓存命中时用），带全部必含条件：现在已不可见的块不返回。
func (s *PG) Chunks(ctx context.Context, version string, f search.Filter, chunkIDs []string) (map[string]search.Candidate, error) {
	meta, metaArgs := metaWhere(f.Meta, 6)
	rows, err := s.pool.Query(ctx, `
		SELECT `+candidateCols+`, 0::float8 FROM `+candidateFrom+`
		WHERE `+mandatory+recallOnly+` AND c.chunk_id = ANY($5::text[])`+meta,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, chunkIDs}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	cands, err := collect(rows)
	if err != nil {
		return nil, err
	}
	out := make(map[string]search.Candidate, len(cands))
	for _, c := range cands {
		out[c.ChunkID] = c
	}
	return out, nil
}

// Sparse 按学习型稀疏向量的内积召回（<#> 是负内积），没有稀疏向量的块不参与；分数是内积 p 归一化成 p / (p + 1)。
func (s *PG) Sparse(ctx context.Context, version string, f search.Filter, vec map[uint32]float32, k int) ([]search.Candidate, error) {
	el := make(map[int32]float32, len(vec))
	for id, w := range vec {
		if id >= sparseDim {
			continue
		}
		el[int32(id)] = w
	}
	meta, metaArgs := metaWhere(f.Meta, 7)
	rows, err := s.pool.Query(ctx, `
		SELECT `+candidateCols+`, greatest(0, -(c.sparse <#> $5)) / (greatest(0, -(c.sparse <#> $5)) + 1)
		FROM `+candidateFrom+`
		WHERE `+mandatory+recallOnly+` AND c.sparse IS NOT NULL`+meta+`
		ORDER BY c.sparse <#> $5, c.chunk_id
		LIMIT $6`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, pgvector.NewSparseVectorFromMap(el, sparseDim), k}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// sparseDim 与 indexing 的 idx.chunks.sparse 列定义一致（bge-m3 的词表大小）
const sparseDim = 250002

// Keyword 按 PostgreSQL 全文检索召回：含任一词即命中，按 ts_rank_cd 排序（标题、标题路径、正文已分权重）。
// 词逐个用 quote_literal 转成 tsquery 的带引号词项，调用方的文本不会被当作 tsquery 语法解析。
func (s *PG) Keyword(ctx context.Context, version string, f search.Filter, terms []string, k int) ([]search.Candidate, error) {
	meta, metaArgs := metaWhere(f.Meta, 7)
	rows, err := s.pool.Query(ctx, `
		WITH q AS (SELECT to_tsquery('simple', string_agg(quote_literal(t), ' | ')) AS q FROM unnest($5::text[]) t)
		SELECT `+candidateCols+`, (ts_rank_cd(c.tsv, q.q) / (ts_rank_cd(c.tsv, q.q) + 1))::float8
		FROM q, `+candidateFrom+`
		WHERE `+mandatory+recallOnly+` AND c.tsv @@ q.q`+meta+`
		ORDER BY ts_rank_cd(c.tsv, q.q) DESC, c.chunk_id
		LIMIT $6`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, terms, k}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// Identifier 按标识符原样匹配正文与标题（不区分大小写）。strpos 不解析通配符，调用方的文本不会被当作模式。
// 开发测试后端逐块比较；数据量上来后由候选后端的精确词项字段承担（技术设计文档 12.7）。
func (s *PG) Identifier(ctx context.Context, version string, f search.Filter, ids []string, k int) ([]search.Candidate, error) {
	meta, metaArgs := metaWhere(f.Meta, 7)
	rows, err := s.pool.Query(ctx, `
		WITH ids AS (SELECT upper(i) AS i FROM unnest($5::text[]) i)
		SELECT `+candidateCols+`, 1::float8 FROM `+candidateFrom+`
		WHERE `+mandatory+recallOnly+meta+`
		  AND EXISTS (SELECT 1 FROM ids WHERE strpos(upper(c.text), ids.i) > 0 OR strpos(upper(d.title), ids.i) > 0)
		ORDER BY (SELECT bool_or(strpos(upper(d.title), ids.i) > 0) FROM ids) DESC, c.chunk_id
		LIMIT $6`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, ids, k}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// LinkEntities 找出名称或别名（归一化后至少 2 字）出现在 text 里的实体。开发测试后端（技术设计文档 12.7
// “PostgreSQL 实体表 + 边表”）逐个比较实体名称；实体多到这样扫不动时换候选里的图后端或加专门的名称索引。
func (s *PG) LinkEntities(ctx context.Context, text string, limit int) ([]search.Entity, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.entity_id, e.canonical_name, e.type FROM idx.entities e
		WHERE EXISTS (SELECT 1 FROM unnest(e.canonical_name || e.aliases) n
			WHERE char_length(regexp_replace(lower(n), '\s', '', 'g')) >= 2
			  AND strpos($1, regexp_replace(lower(n), '\s', '', 'g')) > 0)
		ORDER BY char_length(e.canonical_name) DESC, e.entity_id
		LIMIT $2`, search.NormName(text), limit*4)
	if err != nil {
		return nil, err
	}
	cands, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (search.Entity, error) {
		var e search.Entity
		var t int16
		err := r.Scan(&e.ID, &e.Name, &t)
		e.Type = commonv1.EntityType(t)
		return e, err
	})
	if err != nil {
		return nil, err
	}
	return search.LinkNames(cands, limit), nil
}

// Edges 取与给定实体相连的边。证据块与块召回走同一组连接和必含条件：只在版本清单列出的文档版本里找，
// 证据块所属文档必须对调用者可见、未打删除标记、不在拒绝名单里，调用方的元数据条件同样生效。
func (s *PG) Edges(ctx context.Context, version string, f search.Filter, entityIDs []string, k int) ([]search.Edge, error) {
	meta, metaArgs := metaWhere(f.Meta, 7)
	rows, err := s.pool.Query(ctx, `
		SELECT r.from_entity, ef.canonical_name, ef.type, r.relation_type, r.to_entity, et.canonical_name, et.type, r.confidence,
			`+candidateCols+`
		FROM `+candidateFrom+`
		JOIN idx.relations r ON r.doc_id = vd.doc_id AND r.doc_version = vd.doc_version AND r.evidence_chunk_id = c.chunk_id
		JOIN idx.entities ef ON ef.entity_id = r.from_entity
		JOIN idx.entities et ON et.entity_id = r.to_entity
		WHERE `+mandatory+recallOnly+` AND (r.from_entity = ANY($5::text[]) OR r.to_entity = ANY($5::text[]))`+meta+`
		ORDER BY r.confidence DESC, c.chunk_id, r.from_entity, r.to_entity
		LIMIT $6`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, entityIDs, k}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (search.Edge, error) {
		var e search.Edge
		var fromType, relType, toType, sourceType, classification int16
		var confidence float32
		var span []byte
		c := &e.Evidence
		err := r.Scan(&e.From.ID, &e.From.Name, &fromType, &relType, &e.To.ID, &e.To.Name, &toType, &confidence,
			&c.ChunkID, &c.DocID, &c.DocVersion, &c.ParentChunkID, &c.HeadingPath, &c.Text, &span,
			&c.Title, &c.SourceURI, &sourceType, &c.Owner, &classification, &c.SourceID, &c.InjectionFlag,
			&c.PII.Ciphertext, &c.PII.WrappedKey, &c.PII.KeyID)
		if err != nil {
			return e, err
		}
		e.From.Type, e.To.Type = commonv1.EntityType(fromType), commonv1.EntityType(toType)
		e.Relation = commonv1.RelationType(relType)
		e.Confidence = float64(confidence)
		c.SourceType = commonv1.SourceType(sourceType)
		c.Classification = commonv1.Classification(classification)
		c.Span, err = decodeSpan(span)
		return e, err
	})
}

// Excerpt 与召回共用选择列和必含条件，只按 chunk_id 与 doc_id 定位：权限在读正文的同一条查询里生效。
func (s *PG) Excerpt(ctx context.Context, version string, f search.Filter, docID, chunkID string) (search.Candidate, bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+candidateCols+`, 0::float8 FROM `+candidateFrom+`
		WHERE `+mandatory+` AND vd.doc_id = $5 AND c.chunk_id = $6`,
		version, f.MaxClassification, f.Tokens, f.Sources, docID, chunkID)
	if err != nil {
		return search.Candidate{}, false, err
	}
	cs, err := collect(rows)
	if err != nil || len(cs) == 0 {
		return search.Candidate{}, false, err
	}
	return cs[0], true, nil
}

// 文档级的列举与计数（K2 详设第 3 节）：与召回共用必含条件，只是不连块表。
const docFrom = `idx.version_docs vd
		JOIN idx.documents d ON d.doc_id = vd.doc_id
		JOIN idx.doc_acl a ON a.doc_id = vd.doc_id`

// CountDocs 最多数到 limit 条：超过上限时调用方只报“不少于”，不必扫完全部。
func (s *PG) CountDocs(ctx context.Context, version string, f search.Filter, limit int) (int64, error) {
	meta, metaArgs := metaWhere(f.Meta, 6)
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM (SELECT 1 FROM `+docFrom+` WHERE `+mandatory+meta+` LIMIT $5) t`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, limit}, metaArgs...)...).Scan(&n)
	return n, err
}

func (s *PG) ListDocs(ctx context.Context, version string, f search.Filter, byDate bool, offset, limit int) ([]search.DocSummary, error) {
	meta, metaArgs := metaWhere(f.Meta, 7)
	order := "d.title, vd.doc_id"
	if byDate {
		order = "d.effective_date DESC NULLS LAST, d.title, vd.doc_id"
	}
	rows, err := s.pool.Query(ctx, `
		SELECT vd.doc_id, vd.doc_version, d.title, a.source_id, d.source_uri, d.source_type, a.classification,
			d.doc_type, d.dept, d.topic_codes, COALESCE(to_char(d.effective_date, 'YYYY-MM-DD'), ''), d.validity_status,
			d.metadata_missing, d.vocabulary_version, d.superseded_by,
			COALESCE((SELECT c.chunk_id FROM idx.chunks c
				WHERE c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version AND c.kind = 'summary'), '')
		FROM `+docFrom+` WHERE `+mandatory+meta+`
		ORDER BY `+order+`
		OFFSET $5 LIMIT $6`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, offset, limit}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (search.DocSummary, error) {
		var d search.DocSummary
		var sourceType, classification, validity int16
		md := &commonv1.DocMetadata{}
		err := r.Scan(&d.DocID, &d.DocVersion, &d.Title, &d.SourceID, &d.SourceURI, &sourceType, &classification,
			&md.DocType, &md.Dept, &md.TopicCodes, &md.EffectiveDate, &validity, &md.Missing, &md.VocabularyVersion,
			&md.SupersededBy, &d.SummaryChunkID)
		d.SourceType = commonv1.SourceType(sourceType)
		d.Classification = commonv1.Classification(classification)
		md.ValidityStatus = commonv1.ValidityStatus(validity)
		d.Metadata = md
		return d, err
	})
}

// 分组键：字段为空的文档归入空字符串一组
var groupKeys = map[string]string{
	"doc_type":        "d.doc_type",
	"dept":            "d.dept",
	"validity_status": "d.validity_status::text",
	"year":            "COALESCE(extract(year FROM d.effective_date)::int::text, '')",
}

func (s *PG) CountGroups(ctx context.Context, version string, f search.Filter, by string, limit int) ([]search.Group, error) {
	key, ok := groupKeys[by]
	if !ok {
		return nil, fmt.Errorf("不支持按 %q 分组", by)
	}
	meta, metaArgs := metaWhere(f.Meta, 6)
	rows, err := s.pool.Query(ctx, `
		SELECT `+key+` AS v, count(*) FROM `+docFrom+` WHERE `+mandatory+meta+`
		GROUP BY v ORDER BY count(*) DESC, v LIMIT $5`,
		append([]any{version, f.MaxClassification, f.Tokens, f.Sources, limit}, metaArgs...)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (search.Group, error) {
		var g search.Group
		err := r.Scan(&g.Value, &g.Count)
		return g, err
	})
}

// metaWhere 把元数据条件拼成 “AND …” 子句，参数编号从 start 开始；值一律走参数，不拼进 SQL。
func metaWhere(m search.MetaFilter, start int) (string, []any) {
	var sql strings.Builder
	var args []any
	add := func(clause string, arg any) {
		args = append(args, arg)
		fmt.Fprintf(&sql, " AND "+clause, start+len(args)-1)
	}
	if len(m.DocTypes) > 0 {
		add("d.doc_type = ANY($%d::text[])", m.DocTypes)
	}
	if len(m.Depts) > 0 {
		add("d.dept = ANY($%d::text[])", m.Depts)
	}
	if len(m.TopicCodes) > 0 {
		add("d.topic_codes && $%d::text[]", m.TopicCodes)
	}
	if m.EffectiveFrom != "" {
		add("d.effective_date >= $%d::date", m.EffectiveFrom)
	}
	if m.EffectiveTo != "" {
		add("d.effective_date <= $%d::date", m.EffectiveTo)
	}
	if len(m.Validity) > 0 {
		add("d.validity_status = ANY($%d::int[])", m.Validity)
	}
	if m.Missing != "" {
		add("$%d = ANY(d.metadata_missing)", m.Missing)
	}
	return sql.String(), args
}

// collect 读 candidateCols 加最后一列这一路的归一化分数（Candidate.RouteScore）。
func collect(rows pgx.Rows) ([]search.Candidate, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (search.Candidate, error) {
		var c search.Candidate
		var span []byte
		var sourceType, classification int16
		err := r.Scan(&c.ChunkID, &c.DocID, &c.DocVersion, &c.ParentChunkID, &c.HeadingPath, &c.Text, &span,
			&c.Title, &c.SourceURI, &sourceType, &c.Owner, &classification, &c.SourceID, &c.InjectionFlag,
			&c.PII.Ciphertext, &c.PII.WrappedKey, &c.PII.KeyID, &c.RouteScore)
		if err != nil {
			return c, err
		}
		c.SourceType = commonv1.SourceType(sourceType)
		c.Classification = commonv1.Classification(classification)
		c.Span, err = decodeSpan(span)
		return c, err
	})
}

// 权限读路径。P0 只有一个 PostgreSQL 实例，副本回放位点 ≥ 撤权位点天然成立；
// P2 接只读副本时在这两个函数里先校验位点再读（元数据库可用性详细设计第 4–5 节），调用方不变。

// denied：文档本身或它所属的数据源在紧急拒绝名单里（返回前复核与召回过滤读同一份名单）
const denied = `(EXISTS (SELECT 1 FROM idx.deny_list dl WHERE dl.doc_id = a.doc_id)
			OR EXISTS (SELECT 1 FROM idx.deny_scopes ds WHERE ds.kind = 'source' AND ds.value = a.source_id))`

// TenantDenied 判断租户是否被紧急拒绝：被拒绝的租户任何人都检索不到任何文档。
func (s *PG) TenantDenied(ctx context.Context, tenant string) (bool, error) {
	var denied bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM idx.deny_scopes WHERE kind = 'tenant' AND value = $1)`, tenant).Scan(&denied)
	return denied, err
}

// PartitionTokens 按主体 token 展开可见的分区 token（idx.partition_grants，由 indexing 的快速通道写入）。
func (s *PG) PartitionTokens(ctx context.Context, tokens []string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT 'part:' || source_id || '/' || partition FROM idx.partition_grants
		WHERE grantee = ANY($1::text[]) ORDER BY 1`, tokens)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *PG) DocACL(ctx context.Context, docIDs []string) (map[string]search.ACL, error) {
	return s.acl(ctx, `
		SELECT doc_id, doc_id, source_id, acl_tokens, classification, state = 'active', `+denied+`
		FROM idx.doc_acl a WHERE doc_id = ANY($1::text[])`, docIDs)
}

func (s *PG) ChunkACL(ctx context.Context, chunkIDs []string) (map[string]search.ACL, error) {
	// 同一 chunk_id 可能有多个 doc_version，它们属于同一份文档，权限相同
	return s.acl(ctx, `
		SELECT DISTINCT ON (c.chunk_id) c.chunk_id, a.doc_id, a.source_id, a.acl_tokens, a.classification, a.state = 'active', `+denied+`
		FROM idx.chunks c JOIN idx.doc_acl a USING (doc_id)
		WHERE c.chunk_id = ANY($1::text[])`, chunkIDs)
}

func (s *PG) acl(ctx context.Context, sql string, ids []string) (map[string]search.ACL, error) {
	rows, err := s.pool.Query(ctx, sql, ids)
	if err != nil {
		return nil, err
	}
	out := map[string]search.ACL{}
	for rows.Next() {
		// 每行用新变量扫描，避免切片在行之间被复用
		var key string
		var a search.ACL
		var classification int16
		if err := rows.Scan(&key, &a.DocID, &a.SourceID, &a.Tokens, &classification, &a.Active, &a.Denied); err != nil {
			rows.Close()
			return nil, err
		}
		a.Classification = int32(classification)
		out[key] = a
	}
	return out, rows.Err()
}

func (s *PG) Groups(ctx context.Context, username string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT grp FROM identity.identity_groups WHERE username = $1 ORDER BY grp`, username)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func decodeSpan(raw []byte) (*commonv1.Span, error) {
	var s struct {
		CharStart int32  `json:"char_start"`
		CharEnd   int32  `json:"char_end"`
		LineStart *int32 `json:"line_start"`
		LineEnd   *int32 `json:"line_end"`
		Page      *int32 `json:"page"`
		// 版面类来源才有：[x0, y0, x1, y1]（indexing 写入时已校验）
		Bbox []float32 `json:"bbox"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	out := &commonv1.Span{CharStart: s.CharStart, CharEnd: s.CharEnd, LineStart: s.LineStart, LineEnd: s.LineEnd, Page: s.Page}
	switch len(s.Bbox) {
	case 0:
	case 4:
		out.Bbox = &commonv1.BoundingBox{X0: s.Bbox[0], Y0: s.Bbox[1], X1: s.Bbox[2], Y1: s.Bbox[3]}
	default:
		return nil, fmt.Errorf("span.bbox 应有 4 个坐标，实际 %d 个", len(s.Bbox))
	}
	return out, nil
}
