package writer

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

// maxEntityName 是实体名称的字数上限：再长多半是规则把整句当成了名称，宁可拒收这份文档的抽取结果也不入图。
const maxEntityName = 64

// entityPrefix 是实体主键的类型前缀，主键 = 前缀 + ":" + 归一化名称。
var entityPrefix = map[commonv1.EntityType]string{
	commonv1.EntityType_ENTITY_TYPE_ORG:           "org",
	commonv1.EntityType_ENTITY_TYPE_PRODUCT:       "product",
	commonv1.EntityType_ENTITY_TYPE_POLICY_CLAUSE: "policy",
	commonv1.EntityType_ENTITY_TYPE_SYSTEM:        "system",
	commonv1.EntityType_ENTITY_TYPE_ROLE:          "role",
}

// normName 去掉空白并转小写：“清算核心 服务”与“清算核心服务”是同一实体。retrieval 的实体链接用同一规则。
func normName(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), ""))
}

func entityID(e *ingestionv1.Entity) string {
	return entityPrefix[e.GetType()] + ":" + normName(e.GetName())
}

// validateRelations 检查关系的溯源：证据块必须是本文档这次写入的块，否则图召回会返回无法定位原文的结果。
// 任何一条不合格整份拒收，与块的校验一致。
func validateRelations(r *ingestionv1.WriteDocumentRequest) error {
	chunks := map[string]bool{}
	for _, c := range r.GetChunks() {
		chunks[c.GetChunkId()] = true
	}
	for i, rel := range r.GetRelations() {
		for _, e := range []*ingestionv1.Entity{rel.GetSubject(), rel.GetObject()} {
			if err := validEntity(e); err != nil {
				return fmt.Errorf("第 %d 条关系: %w", i, err)
			}
		}
		switch {
		case entityID(rel.GetSubject()) == entityID(rel.GetObject()):
			return fmt.Errorf("第 %d 条关系: 主体与客体是同一实体", i)
		case rel.GetRelationType() == commonv1.RelationType_RELATION_TYPE_UNSPECIFIED || commonv1.RelationType_name[int32(rel.GetRelationType())] == "":
			return fmt.Errorf("第 %d 条关系: 关系类型 %d 不合法", i, rel.GetRelationType())
		case !chunks[rel.GetEvidenceChunkId()]:
			return fmt.Errorf("第 %d 条关系: 证据块 %q 不在本文档的块里", i, rel.GetEvidenceChunkId())
		case !(rel.GetConfidence() > 0 && rel.GetConfidence() <= 1):
			return fmt.Errorf("第 %d 条关系: 置信度 %v 应在 (0, 1]", i, rel.GetConfidence())
		case rel.GetExtractorVersion() == "":
			return fmt.Errorf("第 %d 条关系: 缺少抽取器版本", i)
		}
	}
	return nil
}

func validEntity(e *ingestionv1.Entity) error {
	if _, ok := entityPrefix[e.GetType()]; !ok {
		return fmt.Errorf("实体类型 %d 不合法", e.GetType())
	}
	for _, n := range append([]string{e.GetName()}, e.GetAliases()...) {
		if l := utf8.RuneCountInString(normName(n)); l < 2 || l > maxEntityName {
			return fmt.Errorf("实体名称 %q 应为 2–%d 字", n, maxEntityName)
		}
	}
	return nil
}

// writeRelations 用请求里的关系整体替换这份文档这个内容版本的关系。只动 (doc_id, doc_version) 自己的行：
// 旧内容版本的关系留给仍引用它的历史索引版本（回滚用），与块的处理方式一致。
func writeRelations(ctx context.Context, tx pgx.Tx, docID string, docVersion int64, rels []*ingestionv1.Relation) error {
	if _, err := tx.Exec(ctx, `DELETE FROM idx.relations WHERE doc_id = $1 AND doc_version = $2`, docID, docVersion); err != nil {
		return err
	}
	if len(rels) == 0 {
		return nil
	}
	entities := map[string]*ingestionv1.Entity{}
	for _, r := range rels {
		for _, e := range []*ingestionv1.Entity{r.GetSubject(), r.GetObject()} {
			id := entityID(e)
			prev, ok := entities[id]
			if !ok {
				prev = &ingestionv1.Entity{Type: e.GetType(), Name: e.GetName()}
				entities[id] = prev
			}
			for _, a := range e.GetAliases() {
				if normName(a) != normName(prev.GetName()) && !slices.Contains(prev.Aliases, a) {
					prev.Aliases = append(prev.Aliases, a)
				}
			}
		}
	}
	// 按主键顺序写实体：热门实体（如同一个核心系统）会被并发的写入事务同时更新，顺序一致才不会死锁
	ids := make([]string, 0, len(entities))
	for id := range entities {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		e := entities[id]
		// 已存在的实体不加锁（DO NOTHING），只在有新别名时才更新，减少并发写入之间的等待
		if _, err := tx.Exec(ctx, `
			INSERT INTO idx.entities (entity_id, type, canonical_name, aliases) VALUES ($1, $2, $3, $4)
			ON CONFLICT (entity_id) DO NOTHING`, id, int32(e.GetType()), e.GetName(), nonNil(e.GetAliases())); err != nil {
			return err
		}
		if len(e.GetAliases()) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE idx.entities SET aliases = ARRAY(SELECT DISTINCT a FROM unnest(aliases || $2::text[]) AS a ORDER BY 1), updated_at = now()
				WHERE entity_id = $1 AND NOT ($2::text[] <@ aliases)`, id, e.GetAliases()); err != nil {
				return err
			}
		}
	}
	// 同一关系同一证据块被多个抽取器给出时只留置信度最高的一条
	sorted := slices.Clone(rels)
	slices.SortStableFunc(sorted, func(a, b *ingestionv1.Relation) int {
		switch {
		case a.GetConfidence() > b.GetConfidence():
			return -1
		case a.GetConfidence() < b.GetConfidence():
			return 1
		}
		return 0
	})
	for _, r := range sorted {
		if _, err := tx.Exec(ctx, `
			INSERT INTO idx.relations (doc_id, doc_version, from_entity, relation_type, to_entity, evidence_chunk_id, confidence, extractor_version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT DO NOTHING`,
			docID, docVersion, entityID(r.GetSubject()), int32(r.GetRelationType()), entityID(r.GetObject()),
			r.GetEvidenceChunkId(), r.GetConfidence(), r.GetExtractorVersion()); err != nil {
			return err
		}
	}
	return nil
}
