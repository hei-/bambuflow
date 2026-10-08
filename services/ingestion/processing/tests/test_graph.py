from collections.abc import Sequence

from bambuflow_processing.graph import (
    ORG,
    POLICY,
    PRODUCT,
    ROLE,
    SYSTEM,
    ChunkText,
    Entity,
    Extractor,
    Relation,
    to_proto,
)
from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

from bambuflow.common.v1 import types_pb2

VOCAB = Vocabularies(
    "vocab-test",
    Vocabulary([Term("DT-POLICY", ("制度", "管理办法", "办法"), "")]),
    Vocabulary(
        [
            Term("DEPT-CREDIT", ("信贷管理部", "信贷部"), ""),
            Term("DEPT-OPS", ("运营管理部", "运营部"), ""),
        ]
    ),
    Vocabulary([Term("TOPIC-RETAIL-CREDIT", ("零售信贷", "个人贷款"), "")]),
)
WIKI = types_pb2.SOURCE_TYPE_WIKI
INTERNAL = types_pb2.CLASSIFICATION_INTERNAL


def triples(rels: Sequence[Relation]) -> set[tuple[str, str, str]]:
    return {
        (r.subject.name, types_pb2.RelationType.Name(r.relation_type)[14:], r.object.name)
        for r in rels
    }


def run(title: str, *texts: str, extractor: Extractor | None = None) -> list[Relation]:
    chunks = [ChunkText(f"src:doc#{i}", t) for i, t in enumerate(texts)]
    return (extractor or Extractor(VOCAB)).extract(title, chunks, WIKI, INTERNAL)


def test_dependency_and_responsibility_sentences() -> None:
    rels = run(
        "清算核心服务说明",
        "目前清算核心服务依赖账户系统和支付网关平台。",
        "运营部负责账户系统的日常运维与变更审批。",
        "支付网关平台由信息科技部负责。",
    )
    assert triples(rels) == {
        ("清算核心服务", "DEPENDS_ON", "账户系统"),
        ("清算核心服务", "DEPENDS_ON", "支付网关平台"),
        # 部门按受控词表归一成规范名称，原文说法记为别名
        ("运营管理部", "RESPONSIBLE_FOR", "账户系统"),
        ("信息科技部", "RESPONSIBLE_FOR", "支付网关平台"),
    }
    ops = next(r.subject for r in rels if r.subject.name == "运营管理部")
    assert ops.type == ORG and ops.aliases == ("运营部",)
    by_chunk = {r.object.name: r.evidence_chunk_id for r in rels}
    assert by_chunk["支付网关平台"] in ("src:doc#0", "src:doc#2")
    assert all(0 < r.confidence <= 1 and r.extractor_version == "rules-1" for r in rels)


def test_policy_document_cites_and_applies_to() -> None:
    rels = run(
        "个人消费贷款管理办法（2025 版）",
        "本办法适用于本行向自然人发放的、用于个人消费用途的人民币贷款。",
        "额度测算依据《授信管理办法》第十二条执行。",
    )
    assert triples(rels) == {
        # 文档实体取去掉版本括注的标题，与别的文档里引用它的《》名称是同一个实体
        ("个人消费贷款管理办法", "APPLIES_TO", "人民币贷款"),
        ("个人消费贷款管理办法", "CITES", "授信管理办法"),
        ("个人消费贷款管理办法", "CITES", "授信管理办法第十二条"),
        ("授信管理办法第十二条", "BELONGS_TO", "授信管理办法"),
    }
    types = {r.object.name: r.object.type for r in rels}
    assert types["人民币贷款"] == PRODUCT and types["授信管理办法"] == POLICY


def test_notice_citing_policy() -> None:
    rels = run(
        "华东分行关于执行《个人消费贷款管理办法》的补充通知",
        "各支行办理相关业务时，额度一律按总行《个人消费贷款管理办法》执行。",
    )
    assert triples(rels) == {
        (
            "华东分行关于执行《个人消费贷款管理办法》的补充通知",
            "CITES",
            "个人消费贷款管理办法",
        )
    }


def test_rules_skip_what_they_cannot_type() -> None:
    rels = run(
        "信息安全事件报告流程",
        # “负责人”是角色，不是“负责”这个动词；“调阅须经”不是名称的一部分
        "调阅须经信贷部负责人审批。",
        "抵押权证由分行押品管理中心集中保管。",
        "客户信息泄露属于重大事件。",
        "本系统依赖人工复核。",
    )
    assert rels == []


def test_role_responsible_for() -> None:
    rels = run("柜面业务规程", "长款挂账由网点负责人负责跟踪。")
    assert triples(rels) == set()  # “长款挂账”不是可判断类型的实体
    rels = run("柜面业务规程", "网点负责人负责尾箱系统的日终检查。")
    assert triples(rels) == {("网点负责人", "RESPONSIBLE_FOR", "尾箱系统")}
    assert rels[0].subject.type == ROLE and rels[0].object.type == SYSTEM


def test_log_data_is_skipped() -> None:
    chunks = [ChunkText("src:log#0", "清算核心服务依赖账户系统")]
    assert Extractor(VOCAB).extract("告警日志", chunks, types_pb2.SOURCE_TYPE_LOG, INTERNAL) == []


class FakeModel:
    version = "model-test-1"

    def __init__(self, out: list[Relation] | None = None, fail: bool = False) -> None:
        self.out = out or []
        self.fail = fail
        self.classifications: list[int] = []

    def extract(
        self, title: str, chunks: Sequence[ChunkText], classification: int
    ) -> list[Relation]:
        self.classifications.append(classification)
        if self.fail:
            raise OSError("网关不可用")
        return self.out


def test_model_port_output_is_filtered_and_failures_fall_back_to_rules() -> None:
    a, b = Entity(SYSTEM, "风控引擎平台"), Entity(SYSTEM, "征信查询服务")
    good = Relation(a, types_pb2.RELATION_TYPE_DEPENDS_ON, b, "src:doc#0", 0.7, "model-test-1")
    model = FakeModel(
        [
            good,
            # 证据块不属于本文档、置信度不够、名称是整句话：都丢弃
            Relation(a, types_pb2.RELATION_TYPE_DEPENDS_ON, b, "src:other#0", 0.9, "m"),
            Relation(a, types_pb2.RELATION_TYPE_DEPENDS_ON, b, "src:doc#0", 0.3, "m"),
            Relation(
                a,
                types_pb2.RELATION_TYPE_DEPENDS_ON,
                Entity(SYSTEM, "调用，然后"),
                "src:doc#0",
                0.9,
                "m",
            ),
        ]
    )
    rels = run("说明", "风控引擎平台的说明。", extractor=Extractor(VOCAB, model))
    assert rels == [good]
    # 密级交给端口实现，由它按密级路由
    assert model.classifications == [INTERNAL]
    rels = run(
        "说明", "清算核心服务依赖账户系统。", extractor=Extractor(VOCAB, FakeModel(fail=True))
    )
    assert triples(rels) == {("清算核心服务", "DEPENDS_ON", "账户系统")}


def test_to_proto() -> None:
    (msg,) = to_proto(run("说明", "清算核心服务依赖账户系统。"))
    assert msg.subject.type == types_pb2.ENTITY_TYPE_SYSTEM and msg.subject.name == "清算核心服务"
    assert msg.relation_type == types_pb2.RELATION_TYPE_DEPENDS_ON
    assert msg.evidence_chunk_id == "src:doc#0" and msg.extractor_version == "rules-1"
