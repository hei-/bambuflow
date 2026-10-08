"""实体与关系抽取（入库管道详设第 7 节，技术设计文档 6.3）：产出图召回用的边，
每条边挂一个证据块和置信度。

本步只做规则和受控词表能确定的部分：
- 实体类型按名称后缀判断（系统 / 服务 / 平台、部 / 中心 / 委员会、经理 / 负责人 / 岗、
  贷款 / 信用卡等），部门名称能被受控词表归一的直接取规范名称，原文说法记为别名；
- 关系只认明确的句式：“A 依赖 / 调用 B”“A 属于 / 隶属于 B”“B 由 A 负责 / 归口管理”“A 负责 B”、
  “本办法适用于 B”、正文里出现的《制度名》（本文档引用它）。
规则宁缺毋滥：说不准类型的名称不入图，因为错误的边会把无关的证据带进回答。

模型抽取经 RelationModel 端口接入（ADR-13：外部系统经端口接入并自带默认实现），默认不接：
设计要求它经 LLM 网关按文档密级路由、受限级走独立实例、用量计入租户配额，这些都由网关侧的实现负责；
接入前只有规则抽取的结果，图召回照常可用，只是覆盖的句式少。
日志类数据不抽取（入库管道详设第 7 节）。
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from bambuflow_vocabulary import Vocabularies

from bambuflow.common.v1 import types_pb2
from bambuflow.ingestion.v1 import index_writer_pb2

RULES_VERSION = "rules-1"
# 模型给出的关系低于这个置信度不入图：错误的边比缺一条边的代价大
MODEL_MIN_CONFIDENCE = 0.6
_MAX_NAME = 32

_log = logging.getLogger(__name__)

ORG = types_pb2.ENTITY_TYPE_ORG
PRODUCT = types_pb2.ENTITY_TYPE_PRODUCT
POLICY = types_pb2.ENTITY_TYPE_POLICY_CLAUSE
SYSTEM = types_pb2.ENTITY_TYPE_SYSTEM
ROLE = types_pb2.ENTITY_TYPE_ROLE

# 后缀 → 类型。先比长的后缀：“理财产品”不能按“产品”以外的规则判断，“负责人”比“人”具体
_SUFFIXES = sorted(
    [
        *((s, SYSTEM) for s in ("系统", "服务", "平台", "网关")),
        *((s, ORG) for s in ("部", "中心", "委员会", "团队", "小组", "分行", "支行", "办公室")),
        *(
            (s, ROLE)
            for s in ("经理", "专员", "负责人", "主管", "管理员", "审批人", "行长", "柜员", "岗")
        ),
        *((s, PRODUCT) for s in ("贷款", "信用卡", "借记卡", "理财产品", "存款产品", "保险产品")),
    ],
    key=lambda x: -len(x[0]),
)
# 制度文档：标题以这些词结尾时，“本办法”“引用《…》”的主体就是这份文档
_POLICY_TITLE = re.compile(r"(办法|制度|规定|细则|规程|规范|指引|准则|流程|通知)$")
_TITLE_SUFFIX = re.compile(r"[（(][^）)]*[）)]\s*$")
_CITATION = re.compile(r"《([^《》]{2,40})》(第[一二三四五六七八九十百零〇\d]+条)?")
# 名称左侧常见的虚词，不属于名称本身
_LEADING = re.compile(
    r"^(?:目前|当前|现在|其中|另外|此外|同时|并且|以及|和|与|及|由|该|本|此|各|所有)+"
)
_CONJ = re.compile(r"[和与及、]|以及")
# 情态、介词等功能字：名称里不会有，出现时说明规则把半句话当成了名称。主体一侧取它们之后的部分，
# 客体一侧取之前的部分：“调阅须经信贷部”取“信贷部”
# 只列不会出现在名称里的写法：“应用系统”“可疑交易监测系统”“个人经营贷款”里的单字不能当功能字
_FUNCTION = re.compile(r"须经|应经|需经|应当|须|需要|将|被|把|是|请")
_CLAUSE = re.compile(r"[。；;！!？?\n，,：:（）()]")
_SELF = re.compile(r"^本(?:办法|制度|规定|细则|规程|规范|指引|准则|流程|通知)")

_DEPENDS = re.compile(r"^(.+?)(?:依赖于|依赖|调用|依托)(.+)$")
_BELONGS = re.compile(r"^(.+?)(?:隶属于|归属于|属于|隶属)(.+)$")
# “负责人”是角色名的一部分，不是动词
_BY_RESPONSIBLE = re.compile(r"^(.+?)由(.+?)(?:负责(?!人)|归口管理|牵头管理|统一管理)")
_RESPONSIBLE = re.compile(r"^(.+?)(?:负责(?!人)|归口管理|牵头管理)(.+)$")
_APPLIES = re.compile(r"^(.+?)适用于(.+)$")


@dataclass(frozen=True)
class Entity:
    type: types_pb2.EntityType
    name: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class Relation:
    subject: Entity
    relation_type: types_pb2.RelationType
    object: Entity
    evidence_chunk_id: str
    confidence: float
    extractor_version: str = RULES_VERSION


@dataclass(frozen=True)
class ChunkText:
    chunk_id: str
    text: str


class RelationModel(Protocol):
    """模型抽取端口：实现方经 LLM 网关调用模型，按 classification 路由（受限级用独立实例）。"""

    version: str

    def extract(
        self, title: str, chunks: Sequence[ChunkText], classification: int
    ) -> list[Relation]: ...


@dataclass
class Extractor:
    vocab: Vocabularies
    # 不配置时只有规则抽取（默认关闭模型抽取）
    model: RelationModel | None = None
    _cache: dict[str, Entity | None] = field(default_factory=dict, init=False)

    def extract(
        self,
        title: str,
        chunks: Sequence[ChunkText],
        source_type: int,
        classification: int,
    ) -> list[Relation]:
        if source_type == types_pb2.SOURCE_TYPE_LOG:
            return []
        doc = _document_entity(title)
        out: list[Relation] = []
        for c in chunks:
            out.extend(self._rules(doc, c))
        if self.model is not None:
            out.extend(_from_model(self.model, title, chunks, classification))
        return _dedupe(out)

    def _rules(self, doc: Entity | None, c: ChunkText) -> list[Relation]:
        out: list[Relation] = []

        def add(
            s: Entity | None, rel: types_pb2.RelationType, o: Entity | None, conf: float
        ) -> None:
            if s and o and _key(s) != _key(o):
                out.append(Relation(s, rel, o, c.chunk_id, conf))

        if doc:
            for cm in _CITATION.finditer(c.text):
                cited = cm.group(1).strip()
                if _norm(cited) == _norm(doc.name):
                    continue
                add(doc, types_pb2.RELATION_TYPE_CITES, Entity(POLICY, cited), 0.95)
                if cm.group(2):
                    article = Entity(POLICY, cited + cm.group(2))
                    add(doc, types_pb2.RELATION_TYPE_CITES, article, 0.95)
                    add(article, types_pb2.RELATION_TYPE_BELONGS_TO, Entity(POLICY, cited), 0.95)
        for clause in (x.strip() for x in _CLAUSE.split(c.text)):
            if not clause:
                continue
            if m := _DEPENDS.match(clause):
                for s in self._entities(m.group(1), doc, subject=True):
                    for o in self._entities(m.group(2), doc):
                        if s.type == SYSTEM and o.type == SYSTEM:
                            add(s, types_pb2.RELATION_TYPE_DEPENDS_ON, o, self._conf(s, o))
                continue
            if m := _BELONGS.match(clause):
                for s in self._entities(m.group(1), doc, subject=True):
                    for o in self._entities(m.group(2), doc):
                        if o.type in (ORG, SYSTEM, POLICY):
                            add(s, types_pb2.RELATION_TYPE_BELONGS_TO, o, self._conf(s, o))
                continue
            if m := _BY_RESPONSIBLE.match(clause):
                for o in self._entities(m.group(1), doc, subject=True):
                    for s in self._entities(m.group(2), doc):
                        if s.type in (ORG, ROLE):
                            add(s, types_pb2.RELATION_TYPE_RESPONSIBLE_FOR, o, self._conf(s, o))
                continue
            if m := _RESPONSIBLE.match(clause):
                for s in self._entities(m.group(1), doc, subject=True):
                    for o in self._entities(m.group(2), doc):
                        if s.type in (ORG, ROLE):
                            add(s, types_pb2.RELATION_TYPE_RESPONSIBLE_FOR, o, self._conf(s, o))
                continue
            if (m := _APPLIES.match(clause)) and doc and _SELF.match(m.group(1).strip()):
                for o in self._entities(m.group(2), doc):
                    if o.type in (PRODUCT, ORG):
                        add(doc, types_pb2.RELATION_TYPE_APPLIES_TO, o, self._conf(doc, o))
        return out

    def _entities(self, phrase: str, doc: Entity | None, subject: bool = False) -> list[Entity]:
        """把动词一侧的文字拆成实体：按并列连词切开，每段裁掉功能字带出的半句话后按词表或后缀判断类型，
        判断不了的丢弃。subject 为 True 时是动词左侧（主体），否则是右侧（客体）。"""
        phrase = phrase.strip()
        if doc and _SELF.match(phrase):
            return [doc]
        out: list[Entity] = []
        for p in _CONJ.split(phrase):
            p = _FUNCTION.split(p)[-1] if subject else _object_head(_FUNCTION.split(p)[0])
            if e := self._entity(p):
                out.append(e)
        return out

    def _entity(self, raw: str) -> Entity | None:
        name = _LEADING.sub("", "".join(raw.split()))
        if not name:
            return None
        if name in self._cache:
            return self._cache[name]
        e = self._classify(name)
        self._cache[name] = e
        return e

    def _classify(self, name: str) -> Entity | None:
        if not _plausible(name):
            return None
        if code := self.vocab.dept.exact(name):
            canonical = self.vocab.dept.name(code)
            return Entity(ORG, canonical, (name,) if name != canonical else ())
        for suffix, t in _SUFFIXES:
            if name.endswith(suffix) and len(name) > len(suffix):
                return Entity(t, name)
        return None

    def _conf(self, *es: Entity) -> float:
        # 两端都是词表里的规范名称时更可信；只靠后缀判断类型的低一些
        return 0.9 if all(e.type == ORG and self.vocab.dept.exact(e.name) for e in es) else 0.8


def _from_model(
    model: RelationModel, title: str, chunks: Sequence[ChunkText], classification: int
) -> list[Relation]:
    try:
        found = model.extract(title, chunks, classification)
    except (OSError, ValueError, RuntimeError) as e:
        # 增强失败不阻塞入库（入库管道详设第 7 节）：只用规则的结果，下次重处理时再补
        _log.warning("模型抽取关系失败，只用规则结果", extra={"reason": str(e)})
        return []
    ids = {c.chunk_id for c in chunks}
    # 模型的输出不可信：证据块必须属于本文档、置信度够、名称像个名称，否则丢弃（溯源 fail-closed）
    return [
        r
        for r in found
        if r.evidence_chunk_id in ids
        and MODEL_MIN_CONFIDENCE <= r.confidence <= 1
        and _plausible(r.subject.name)
        and _plausible(r.object.name)
        and r.subject.type != types_pb2.ENTITY_TYPE_UNSPECIFIED
        and r.object.type != types_pb2.ENTITY_TYPE_UNSPECIFIED
    ]


def _object_head(phrase: str) -> str:
    """客体一侧取到第一个“的”之前或之后：“个人消费用途的人民币贷款”取“人民币贷款”，
    “账户系统的日终批量”取“账户系统”。先试最后一个“的”之后的部分，再试第一个“的”之前的部分。"""
    if "的" not in phrase:
        return phrase
    tail = phrase.rsplit("的", 1)[1]
    head = phrase.split("的", 1)[0]
    return tail if any(tail.endswith(s) for s, _ in _SUFFIXES) else head


def _document_entity(title: str) -> Entity | None:
    clean = _TITLE_SUFFIX.sub("", title).strip()
    if _POLICY_TITLE.search(clean) and _plausible(clean):
        return Entity(POLICY, clean, (title,) if title != clean else ())
    return None


def _plausible(name: str) -> bool:
    n = _norm(name)
    return 2 <= len(n) <= _MAX_NAME and not re.search(r"[，。；：、！？,.;:!?\s]", name)


def _norm(s: str) -> str:
    return "".join(s.split()).lower()


def _key(e: Entity) -> tuple[int, str]:
    return e.type, _norm(e.name)


def _dedupe(rels: list[Relation]) -> list[Relation]:
    """同一关系同一证据块只留置信度最高的一条。"""
    best: dict[tuple[tuple[int, str], int, tuple[int, str], str], Relation] = {}
    for r in rels:
        k = (_key(r.subject), r.relation_type, _key(r.object), r.evidence_chunk_id)
        if k not in best or r.confidence > best[k].confidence:
            best[k] = r
    return list(best.values())


def to_proto(rels: Sequence[Relation]) -> list[index_writer_pb2.Relation]:
    def ent(e: Entity) -> index_writer_pb2.Entity:
        return index_writer_pb2.Entity(type=e.type, name=e.name, aliases=list(e.aliases))

    return [
        index_writer_pb2.Relation(
            subject=ent(r.subject),
            relation_type=r.relation_type,
            object=ent(r.object),
            evidence_chunk_id=r.evidence_chunk_id,
            confidence=r.confidence,
            extractor_version=r.extractor_version,
        )
        for r in rels
    ]
