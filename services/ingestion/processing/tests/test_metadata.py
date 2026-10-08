import datetime as dt

from bambuflow_processing.metadata import extract
from bambuflow_vocabulary import Term, Vocabularies, Vocabulary

from bambuflow.common.v1 import types_pb2

VOCAB = Vocabularies(
    "vocab-test",
    Vocabulary(
        [
            Term("DT-POLICY", ("制度", "管理办法", "办法", "实施细则", "细则", "规定"), ""),
            Term("DT-PROCEDURE", ("流程", "规程"), ""),
            Term("DT-NOTICE", ("通知", "公告"), ""),
            Term("DT-MANUAL", ("手册", "员工手册", "须知"), ""),
            Term("DT-RULE", ("旧规定",), "DT-POLICY"),
        ]
    ),
    Vocabulary(
        [
            Term("DEPT-CREDIT", ("信贷管理部", "信贷部"), ""),
            Term("DEPT-RISK", ("风险管理部", "风险部"), ""),
        ]
    ),
    Vocabulary(
        [
            Term("TOPIC-CREDIT", ("信贷", "贷款"), ""),
            Term("TOPIC-RETAIL-CREDIT", ("零售信贷", "个人消费贷款", "个人贷款"), ""),
            Term("TOPIC-AML", ("反洗钱",), ""),
        ]
    ),
)

TODAY = dt.date(2026, 10, 6)


def test_source_fields_win_and_are_normalized() -> None:
    src = {
        "doc_type": "管理办法",
        "dept": "信贷部",
        "topics": ["个人贷款", "没有这个主题"],
        "effective_date": "2025-04-01",
        "status": "effective",
    }
    md = extract("随便一个标题", "", src, VOCAB, TODAY)
    assert (md.doc_type, md.dept, list(md.topic_codes)) == (
        "DT-POLICY",
        "DEPT-CREDIT",
        ["TOPIC-RETAIL-CREDIT"],
    )
    assert (
        md.effective_date == "2025-04-01"
        and md.validity_status == types_pb2.VALIDITY_STATUS_EFFECTIVE
    )
    assert not md.missing and md.vocabulary_version == "vocab-test"


def test_rules_from_title_and_body() -> None:
    body = "第一条 ……\n发文部门：风险管理部\n第三十条 本办法自 2027 年 1 月 1 日起施行。"
    md = extract("个人消费贷款管理办法（2025 版）", body, None, VOCAB, TODAY)
    # 标题末尾“管理办法”比“办法”长，取长的；括注里的版本不影响
    assert md.doc_type == "DT-POLICY" and md.dept == "DEPT-RISK"
    # “个人消费贷款”覆盖了“贷款”：只算更具体的那个主题
    assert list(md.topic_codes) == ["TOPIC-RETAIL-CREDIT"]
    assert md.effective_date == "2027-01-01"
    assert md.validity_status == types_pb2.VALIDITY_STATUS_PENDING  # 生效日期在将来
    assert not md.missing


def test_unknown_fields_are_missing_not_guessed() -> None:
    body = "本制度由风险管理部负责审批。本办法自 2025 年 3 月 1 日起施行。"
    md = extract("员工请假须知", body, {"dept": "不存在的部门"}, VOCAB, TODAY)
    assert md.doc_type == "DT-MANUAL"
    # 源端给了词表里没有的部门：不回退去正文猜；正文里随便出现的部门名也不算发文部门
    assert md.dept == ""
    # 生效日期已过也不能推出“现行”：可能已被后来的文档废止
    assert (
        md.effective_date == "2025-03-01"
        and md.validity_status == types_pb2.VALIDITY_STATUS_UNSPECIFIED
    )
    assert list(md.missing) == ["dept", "topic_codes", "validity_status"]


def test_deprecated_code_is_replaced_and_repealed_title() -> None:
    md = extract("关于信贷业务的旧规定（已废止）", "", {"doc_type": "旧规定"}, VOCAB, TODAY)
    assert md.doc_type == "DT-POLICY" and md.validity_status == types_pb2.VALIDITY_STATUS_REPEALED
