"""意图路由（orchestrator 详设 5.3）：把问题分到单跳、多跳与比较、关系、超出范围几条路径。

先用规则：意图分类小模型尚未部署（详设第 16 节待决问题“小模型还是规则优先 + LLM 兜底”）。
规则只认明确的说法，认不出时按单跳处理：单跳最便宜，判错时证据不足也只会返回
insufficient_evidence，不会给出错误答案。超出范围直接拒答，只认很窄的说法，避免误拒
（详设要求拒答的置信度 ≥ 0.9）。枚举与计数由 listing.understand 判断，在服务入口与这里协调：
超出范围最先判断，其次是清单路径，最后才是这里的其余几类。

结构化问题（问业务数据，需要只读 sql_query）暂不支持：数据平台的只读查询服务尚未接入，
这类问题按单跳处理，知识库里没有依据时返回证据不足。
"""

from __future__ import annotations

import re

from bambuflow.answer.v1 import answer_pb2

# 闲聊与投资建议。只收明确的说法：“理财产品的风险等级”是知识库问题，“推荐一只股票”不是
_OUT_OF_SCOPE = re.compile(
    r"推荐.{0,4}(?:股票|基金|个股)|(?:买|卖|抄底)哪(?:只|支|个)(?:股票|基金)|股价.{0,4}(?:会涨|会跌|走势)"
    r"|讲个笑话|^(?:你好|您好|hello|hi)[呀啊！!。.]*$|你是谁|今天天气|天气怎么样"
)
# 比较：两个对象在某方面的异同
_COMPARE = re.compile(r"区别|差异|异同|不同之处|有何不同|有什么不同|相比|对比|比较|哪个更|孰")
# 需要分别回答几件事，或要先得出中间结论
_MULTI = re.compile(r"分别|各自|各是|先.{1,20}再")
# 一句里问了两个问题：“额度是多少？期限多长？”
_TWO_QUESTIONS = re.compile(r"[？?；;].+[？?]")
# 问单个关系
_RELATION = re.compile(
    r"由哪个(?:团队|部门|系统|小组)|归哪个|谁负责|负责人是|负责团队|属于哪个(?:系统|部门|团队)"
    r"|依赖(?:了)?哪些?(?:系统|服务)|上游|下游"
)


def classify(question: str) -> answer_pb2.Intent:
    """不含清单路径：枚举与计数在调用方先于多跳判断（见模块说明）。"""
    q = question.strip()
    if _OUT_OF_SCOPE.search(q):
        return answer_pb2.INTENT_OUT_OF_SCOPE
    if _COMPARE.search(q) or _MULTI.search(q) or _TWO_QUESTIONS.search(q):
        return answer_pb2.INTENT_MULTI_HOP
    if _RELATION.search(q):
        return answer_pb2.INTENT_RELATION
    return answer_pb2.INTENT_SINGLE_HOP
