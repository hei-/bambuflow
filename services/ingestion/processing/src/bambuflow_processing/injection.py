"""可疑指令标记（入库管道详设第 7 节，技术设计文档 8.5“间接提示词注入”）：
规则加分类器检测正文里的提示词注入，命中的块打 injection_flag。

标记不删除内容：块照常入库、可检索、可引用，只是在线不能成为某句的唯一依据
（orchestrator 详设 7.4、9.2），返回给 agent 时继续携带标记。所以这里宁可多标：
误标的代价是这块不能单独支撑一句话，漏标的代价是注入内容被当作唯一依据。

规则只认明确的说法：要求“忽略 / 无视之前的指令”、改变角色设定、套取系统提示、
伪造对话角色或证据区标记。分类器经 InjectionClassifier 端口接入（ADR-13），默认不接：
设计没有指定模型，由部署方接公司已有的分类服务；分类器不可用时只用规则的结果
（增强失败不阻塞入库）。
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from bambuflow_processing.graph import ChunkText

RULES_VERSION = "rules-1"
# 分类器给出的可疑程度达到它才打标（建议值，接入分类器后按其评测结果校准）
CLASSIFIER_THRESHOLD = 0.8

_log = logging.getLogger(__name__)

_PATTERNS = [
    # 忽略、无视、不要遵守之前的指令或规则
    r"(?:忽略|无视|忘记|忘掉|不要遵守|不必遵守|跳过)(?:掉)?(?:你)?(?:之前|以上|上述|前面|先前|此前|所有|全部|一切)"
    r".{0,6}(?:指令|指示|规则|要求|提示|设定|约束)",
    r"(?:ignore|disregard|forget)\s+(?:all\s+|any\s+)?(?:the\s+)?(?:previous|prior|above|earlier|preceding)"
    r"\s+(?:instructions?|rules?|prompts?|directions?)",
    # 改变角色设定
    r"(?:从现在起|从现在开始|接下来)?你(?:现在)?(?:是|扮演|作为)一?(?:个|名)?(?:不受限制|没有限制|无限制)",
    r"you\s+are\s+now\s+(?:a|an|in)\b",
    r"(?:进入|开启|切换到)(?:开发者|管理员|越狱|DAN)模式",
    # 套取系统提示或隐藏内容
    r"(?:输出|打印|显示|告诉我|泄露|复述)(?:你的)?(?:系统提示|系统提示词|system\s*prompt|初始指令|隐藏指令)",
    r"(?:reveal|print|show|repeat)\s+(?:your\s+)?(?:system\s+prompt|hidden\s+instructions?)",
    # 要求把全部来源、全部内容列出来（越权摸底的常见说法）
    r"(?:把|将)(?:全部|所有)(?:来源|文档|证据|资料).{0,4}(?:列出|输出)",
    # 伪造对话角色或证据区标记
    r"</?\s*(?:evidence|question|system|assistant|document_content)\b",
    r"<\|im_(?:start|end)\|>|\[/?INST\]|###\s*(?:system|instruction)\s*:",
]
_RULE = re.compile("|".join(f"(?:{p})" for p in _PATTERNS), re.IGNORECASE)


class InjectionClassifier(Protocol):
    """分类器端口：对每段文本给出 0–1 的可疑程度。
    实现方按 classification 路由（受限级用独立实例）。"""

    version: str

    def score(self, texts: Sequence[str], classification: int) -> list[float]: ...


@dataclass
class Detector:
    classifier: InjectionClassifier | None = None

    def flagged(self, chunks: Sequence[ChunkText], classification: int) -> frozenset[str]:
        """返回带可疑指令的块。"""
        out = {c.chunk_id for c in chunks if matches(c.text)}
        if self.classifier is None:
            return frozenset(out)
        try:
            scores = self.classifier.score([c.text for c in chunks], classification)
        except (OSError, ValueError, RuntimeError) as e:
            # 增强失败不阻塞入库：只用规则的结果，下次重处理时再补
            _log.warning("注入分类器不可用，只用规则结果", extra={"reason": str(e)})
            return frozenset(out)
        if len(scores) != len(chunks):
            _log.warning("注入分类器返回的条数不对，只用规则结果")
            return frozenset(out)
        out |= {
            c.chunk_id for c, s in zip(chunks, scores, strict=True) if s >= CLASSIFIER_THRESHOLD
        }
        return frozenset(out)


def matches(text: str) -> bool:
    # 去掉空白再匹配一次：“忽 略 以上 指令”这类插空格的写法
    return bool(_RULE.search(text) or _RULE.search(re.sub(r"\s+", "", text)))
