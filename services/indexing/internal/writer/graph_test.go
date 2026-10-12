package writer

import (
	"testing"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
)

func withRelation() *ingestionv1.WriteDocumentRequest {
	r := validRequest()
	r.Relations = []*ingestionv1.Relation{{
		Subject:         &ingestionv1.Entity{Type: commonv1.EntityType_ENTITY_TYPE_SYSTEM, Name: "清算核心服务"},
		RelationType:    commonv1.RelationType_RELATION_TYPE_DEPENDS_ON,
		Object:          &ingestionv1.Entity{Type: commonv1.EntityType_ENTITY_TYPE_SYSTEM, Name: "账户系统", Aliases: []string{"账户 系统"}},
		EvidenceChunkId: r.GetChunks()[0].GetChunkId(), Confidence: 0.8, ExtractorVersion: "rules-1",
	}}
	return r
}

func TestValidateRelations(t *testing.T) {
	if err := validate(withRelation()); err != nil {
		t.Fatalf("合法关系被拒: %v", err)
	}
	cases := map[string]func(r *ingestionv1.Relation){
		// 证据块不在本文档里：图召回会返回无法定位原文、也无法按本文档权限过滤的结果
		"证据块不属于本文档": func(r *ingestionv1.Relation) { r.EvidenceChunkId = "src_wiki:other#a1" },
		"缺证据块":      func(r *ingestionv1.Relation) { r.EvidenceChunkId = "" },
		"置信度为 0":    func(r *ingestionv1.Relation) { r.Confidence = 0 },
		"置信度大于 1":   func(r *ingestionv1.Relation) { r.Confidence = 1.5 },
		"缺关系类型":     func(r *ingestionv1.Relation) { r.RelationType = 0 },
		"关系类型越界":    func(r *ingestionv1.Relation) { r.RelationType = 99 },
		"缺实体类型":     func(r *ingestionv1.Relation) { r.Subject.Type = 0 },
		"缺客体":       func(r *ingestionv1.Relation) { r.Object = nil },
		"名称只有一个字":   func(r *ingestionv1.Relation) { r.Object.Name = "账" },
		"主体与客体相同":   func(r *ingestionv1.Relation) { r.Object.Name = "清算核心 服务" },
		"缺抽取器版本":    func(r *ingestionv1.Relation) { r.ExtractorVersion = "" },
		"别名过长": func(r *ingestionv1.Relation) {
			r.Object.Aliases = []string{string(make([]rune, maxEntityName+1))}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := withRelation()
			mutate(r.Relations[0])
			if err := validate(r); err == nil {
				t.Fatal("应拒收")
			}
		})
	}
}

func TestEntityIDNormalizesName(t *testing.T) {
	a := entityID(&ingestionv1.Entity{Type: commonv1.EntityType_ENTITY_TYPE_SYSTEM, Name: "Core 清算服务"})
	b := entityID(&ingestionv1.Entity{Type: commonv1.EntityType_ENTITY_TYPE_SYSTEM, Name: "core清算服务"})
	if a != b || a != "system:core清算服务" {
		t.Fatalf("同一实体的主键应相同: %q %q", a, b)
	}
	// 同名不同类型是不同实体
	if c := entityID(&ingestionv1.Entity{Type: commonv1.EntityType_ENTITY_TYPE_ORG, Name: "core清算服务"}); c == a {
		t.Fatal("类型不同应是不同实体")
	}
}
