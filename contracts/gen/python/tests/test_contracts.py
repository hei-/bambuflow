"""生成代码的冒烟测试：每个模块都能导入，消息能序列化往返。"""

import importlib
from pathlib import Path

import bambuflow
from bambuflow.answer.v1 import answer_pb2
from bambuflow.common.v1 import types_pb2


def test_all_generated_modules_import() -> None:
    # 生成代码是没有 __init__.py 的命名空间包，pkgutil 不会递归进去，所以直接按文件找。
    modules = [
        "bambuflow." + ".".join(f.relative_to(root).with_suffix("").parts)
        for root in map(Path, bambuflow.__path__)
        for f in root.rglob("*_pb2*.py")
    ]
    assert len(modules) >= 10
    for name in modules:
        importlib.import_module(name)


def test_answer_round_trip() -> None:
    resp = answer_pb2.AnswerResponse(
        answer_id="ans_1",
        status=answer_pb2.STATUS_ANSWERED,
        answer="结论[S1]",
        citations=[
            answer_pb2.Citation(
                ref_id=1,
                chunk_id="wiki:doc-1#a1b2",
                doc_version=3,
                span=types_pb2.Span(char_start=0, char_end=12, line_start=1),
                classification=types_pb2.CLASSIFICATION_INTERNAL,
            )
        ],
    )
    got = answer_pb2.AnswerResponse.FromString(resp.SerializeToString())
    assert got == resp
    assert got.citations[0].span.HasField("line_start")
    assert not got.citations[0].span.HasField("page")
