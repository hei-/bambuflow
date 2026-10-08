from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

from bambuflow.config.v1 import config_pb2

TOPIC = Vocabulary(
    [
        Term("T-CREDIT", ("信贷", "贷款")),
        Term("T-RETAIL", ("零售信贷", "个人消费贷款", "个人贷款"), parent="T-CREDIT"),
        Term("T-MORTGAGE", ("住房贷款",), parent="T-RETAIL"),
        Term("T-OLD", ("旧主题",), replaced_by="T-CREDIT"),
    ]
)


def test_contained_prefers_longest_match() -> None:
    assert TOPIC.contained("个人消费贷款管理办法") == ["T-RETAIL"]
    assert TOPIC.contained("旧主题与住房贷款") == ["T-MORTGAGE", "T-CREDIT"]


def test_descendants_and_replacement() -> None:
    assert TOPIC.with_descendants("T-CREDIT") == ["T-CREDIT", "T-RETAIL", "T-MORTGAGE"]
    assert TOPIC.exact("旧主题") == "T-CREDIT" and TOPIC.suffix("某某住房贷款") == "T-MORTGAGE"
    assert TOPIC.name("T-RETAIL") == "零售信贷"


def test_from_snapshot_requires_all_vocabularies() -> None:
    snap = config_pb2.GetSnapshotResponse(
        snapshot_version="v1",
        vocabularies=[
            config_pb2.Vocabulary(name=n, terms=[config_pb2.VocabularyTerm(code="C", name="名")])
            for n in ("doc_type", "dept", "topic")
        ],
    )
    assert Vocabularies.from_snapshot(snap).doc_type.exact("名") == "C"
    del snap.vocabularies[2]
    try:
        Vocabularies.from_snapshot(snap)
    except ValueError:
        return
    raise AssertionError("缺少词表时应报错")


def test_without_names_blocks_overlap() -> None:
    dept = Vocabulary([Term("D-CREDIT", ("信贷部", "信贷管理部"))])
    assert TOPIC.contained(dept.without_names("信贷管理部的贷款制度")) == ["T-CREDIT"]
    assert TOPIC.contained(dept.without_names("信贷部有哪些制度")) == []
