package api

import (
	"encoding/json"
	"strings"
	"testing"

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	commonv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/common/v1"
)

func TestWhyCarriesIntentAndAgentRounds(t *testing.T) {
	why := whyOf(&answerv1.Why{
		Intent: answerv1.Intent_INTENT_MULTI_HOP,
		AgentRounds: []*answerv1.AgentRound{{Round: 1, Calls: []*answerv1.AgentCall{
			{SubQuestionId: "q1", SubQuestion: "旧版抵押率", Tool: "doc_search", Covered: true, ElapsedMs: 120},
		}}},
	})
	b, _ := json.Marshal(why)
	got := string(b)
	for _, want := range []string{`"intent":"multi_hop"`, `"round":1`, `"sub_question_id":"q1"`, `"chunk_ids":[]`, `"covered":true`, `"elapsed_ms":120`} {
		if !strings.Contains(got, want) {
			t.Errorf("why 缺少 %s: %s", want, got)
		}
	}
	// 旧版 orchestrator 不填意图、单跳没有轮次、没有图召回证据：三个字段都不输出
	b, _ = json.Marshal(whyOf(&answerv1.Why{}))
	if s := string(b); strings.Contains(s, "intent") || strings.Contains(s, "agent_rounds") || strings.Contains(s, "graph_path") {
		t.Errorf("未设置的意图、轮次与图路径不应输出: %s", s)
	}
}

func TestWhyCarriesGraphPath(t *testing.T) {
	why := whyOf(&answerv1.Why{
		Intent: answerv1.Intent_INTENT_RELATION,
		GraphPath: []*answerv1.GraphPath{{RefId: 2, ChunkId: "wiki:doc-9#c1", Edges: []*answerv1.GraphEdge{
			{FromEntity: "清算核心服务", FromType: commonv1.EntityType_ENTITY_TYPE_SYSTEM,
				RelationType: commonv1.RelationType_RELATION_TYPE_DEPENDS_ON, ToEntity: "账户系统", ToType: commonv1.EntityType_ENTITY_TYPE_SYSTEM},
			{FromEntity: "账户系统", FromType: commonv1.EntityType_ENTITY_TYPE_SYSTEM,
				RelationType: commonv1.RelationType_RELATION_TYPE_RESPONSIBLE_FOR, ToEntity: "运营部", ToType: commonv1.EntityType_ENTITY_TYPE_ORG},
		}}},
	})
	b, _ := json.Marshal(why)
	got := string(b)
	for _, want := range []string{`"intent":"relation"`, `"ref_id":2`, `"chunk_id":"wiki:doc-9#c1"`,
		`"from_entity":"清算核心服务","from_type":"system","relation_type":"depends_on","to_entity":"账户系统","to_type":"system"`,
		`"relation_type":"responsible_for","to_entity":"运营部","to_type":"org"`} {
		if !strings.Contains(got, want) {
			t.Errorf("why 缺少 %s: %s", want, got)
		}
	}
}
