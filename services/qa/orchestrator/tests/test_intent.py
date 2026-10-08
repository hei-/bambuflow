from bambuflow.answer.v1 import answer_pb2
from bambuflow_orchestrator import intent

CASES = {
    "跨境汇款单笔上限是多少": answer_pb2.INTENT_SINGLE_HOP,
    "理财产品的风险等级怎么划分？": answer_pb2.INTENT_SINGLE_HOP,
    "新旧两版信贷政策在抵押率上有什么区别": answer_pb2.INTENT_MULTI_HOP,
    "个人贷款与经营贷款相比，审批流程有何不同？": answer_pb2.INTENT_MULTI_HOP,
    "请假和报销分别要提前几天提交？": answer_pb2.INTENT_MULTI_HOP,
    "额度是多少？期限多长？": answer_pb2.INTENT_MULTI_HOP,
    "清算核心服务由哪个团队负责": answer_pb2.INTENT_RELATION,
    "帮我推荐一只股票": answer_pb2.INTENT_OUT_OF_SCOPE,
    "你好": answer_pb2.INTENT_OUT_OF_SCOPE,
    "今天天气怎么样": answer_pb2.INTENT_OUT_OF_SCOPE,
}


def test_classify() -> None:
    for question, want in CASES.items():
        assert intent.classify(question) == want, question


def test_out_of_scope_is_narrow() -> None:
    # 只在明确的说法上拒答：问候之后接着问业务、提到股票的制度问题都按知识库问题处理
    for q in ("你好，请问跨境汇款的上限是多少？", "员工买卖股票有哪些申报要求？"):
        assert intent.classify(q) != answer_pb2.INTENT_OUT_OF_SCOPE, q
