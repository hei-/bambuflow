"""回放时按调用者当前的权限调整记录下来的回答（gateway 详设 4.1：引用按当前权限重新复核）。

回答记录里是当时校验通过的内容；之后文档可能被撤权，查看的人也可能是审计岗而不是提问人。
返回前用调用者本人的身份复核，规则：

1. CheckAccess 不允许的引用从 citations 去掉；
2. 句子的依据（ref_ids、逐句引文 quotes、计算句的取数 calc.inputs）里只要有一条被去掉的引用，
   整句去掉。比“全部依据都不可见才去掉”更严：综合句、计算句由几条引文共同支撑，
   句子里可能写着不可见文档的事实，留下它等于把无权内容带出去（fail-closed）；
3. 列表项引用的是文档本身，按 CheckDocumentAccess 复核，无权的项去掉，
   统计口径块的总数与列出数一起减、口径句按同一模板重写，不暴露“有一份你看不到的文档”
   （K2 详设第 8 节，与 qa/batch 取结果时相同）；
4. why.graph_path 中属于被去掉引用的路径一并去掉：路径上的实体名称来自那份文档；
5. 有内容被去掉、且剩下的句子都不再有依据（evidence_only 时没有剩下任何引用）时，
   status 改为 insufficient_evidence。

回答文本按句子区间（以字符计）重新拼接，句子位置与块的句子下标同步更新；区间不合法时无法安全调整，
抛 MalformedError，由调用方按 fail-closed 处理。
"""

from __future__ import annotations

from collections.abc import Mapping

from google.protobuf.message import Message

from bambuflow.answer.v1 import answer_pb2
from bambuflow_orchestrator import listing


class MalformedError(Exception):
    pass


def _refs(s: answer_pb2.Sentence) -> set[int]:
    refs = set(s.ref_ids)
    refs.update(q.ref_id for q in s.quotes)
    refs.update(i.ref_id for i in s.calc.inputs)
    return refs


def prune(
    a: answer_pb2.AnswerResponse,
    chunks: Mapping[str, bool],
    docs: Mapping[str, bool],
) -> answer_pb2.AnswerResponse:
    """返回调整后的副本。chunks、docs 是复核结果；不在其中的 ID 按不可见处理。"""
    out = answer_pb2.AnswerResponse()
    out.CopyFrom(a)
    hidden = {c.ref_id for c in out.citations if not chunks.get(c.chunk_id, False)}
    drop: set[int] = set()
    unlisted = 0
    for i, s in enumerate(out.sentences):
        if _refs(s) & hidden:
            drop.add(i)
        elif s.HasField("document") and not docs.get(s.document.doc_id, False):
            drop.add(i)
            unlisted += 1
    if not hidden and not drop:
        return out

    replace: dict[int, str] = {}
    if unlisted:
        _shrink_scope(out, unlisted, replace)
    _rewrite(out, drop, replace)
    kept = [_clone(c) for c in out.citations if c.ref_id not in hidden]
    del out.citations[:]
    out.citations.extend(kept)
    paths = [_clone(p) for p in out.why.graph_path if p.ref_id not in hidden]
    del out.why.graph_path[:]
    out.why.graph_path.extend(paths)

    if out.status == answer_pb2.STATUS_EVIDENCE_ONLY:
        if not out.citations:
            out.status = answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
    elif out.status != answer_pb2.STATUS_REFUSED and not any(
        # 程序写的说明（冲突说明也带引用）不算回答内容
        (s.ref_ids or s.HasField("document")) and s.claim_type != answer_pb2.CLAIM_TYPE_NONE
        for s in out.sentences
    ):
        out.status = answer_pb2.STATUS_INSUFFICIENT_EVIDENCE
    return out


def _shrink_scope(a: answer_pb2.AnswerResponse, removed: int, replace: dict[int, str]) -> None:
    """去掉的文档当时已计入总数：总数与列出数一起减，只减列出数会留下“共 N 份、列出 N−1 份”。
    总数不精确（超过上限）时只调列出数。"""
    for b in a.blocks:
        if b.block_type != answer_pb2.BLOCK_TYPE_SCOPE or not b.HasField("scope"):
            continue
        sc = b.scope
        sc.shown = max(0, sc.shown - removed)
        if sc.total_is_exact:
            sc.total_count = max(0, sc.total_count - removed)
        if len(b.sentence_indexes) == 1:
            replace[b.sentence_indexes[0]] = listing.scope_note(sc, a.answer_kind)
        return


def _rewrite(a: answer_pb2.AnswerResponse, drop: set[int], replace: dict[int, str]) -> None:
    """去掉 drop 里的句子及其前面的分隔（第一句被去掉时去掉它后面的分隔），用 replace 替换句子文本，
    再更新句子位置与块的句子下标；句子全被去掉的块一并去掉（口径块保留）。"""
    old = a.answer
    sents = [_clone(s) for s in a.sentences]
    order = sorted(range(len(sents)), key=lambda i: sents[i].start)
    parts: list[str] = []
    length = 0
    pos, skip_gap = 0, False
    for i in order:
        s = sents[i]
        start, end = s.start, s.end
        if start < pos or end < start or end > len(old):
            raise MalformedError(f"句子 {i} 的区间 [{start},{end}) 不合法")
        gap = old[pos:start]
        pos = end
        if i in drop:
            skip_gap = skip_gap or length == 0
            continue
        if not skip_gap:
            parts.append(gap)
            length += len(gap)
        skip_gap = False
        text = replace.get(i, old[start:end])
        if i in replace:
            s.text = text
        s.start = length
        parts.append(text)
        length += len(text)
        s.end = length
    parts.append(old[pos:])
    a.answer = "".join(parts)

    index: dict[int, int] = {}
    kept: list[answer_pb2.Sentence] = []
    for i, s in enumerate(sents):
        if i not in drop:
            index[i] = len(kept)
            kept.append(s)
    del a.sentences[:]
    a.sentences.extend(kept)
    blocks: list[answer_pb2.Block] = []
    for b in a.blocks:
        idx = [index[i] for i in b.sentence_indexes if i in index]
        if idx or b.block_type == answer_pb2.BLOCK_TYPE_SCOPE:
            nb = _clone(b)
            del nb.sentence_indexes[:]
            nb.sentence_indexes.extend(idx)
            blocks.append(nb)
    del a.blocks[:]
    a.blocks.extend(blocks)


def _clone[M: Message](m: M) -> M:
    """重复字段清空后，之前取出的元素引用不保证还能用：先复制出来再改、再放回。"""
    out = type(m)()
    out.CopyFrom(m)
    return out
