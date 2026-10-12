from pathlib import Path

from bambuflow_processing.blocks import Block, assemble, table_text
from bambuflow_processing.chunker import MAX_CHARS, TABLE_MAX_CHARS, chunk, chunk_parsed

BODY = """# 个人授信管理办法

前言：本办法适用于个人客户。

## 第一章 总则

第一条 为规范个人授信业务，制定本办法。

```
# 这不是标题
```

## 第二章 额度

### 第一节 计算

授信额度 = 月收入 × 倍数。

## 第三章 附则

本办法自发布之日起施行。
"""


def test_sections_spans_and_paths() -> None:
    chunks = chunk("src_wiki", "credit", BODY)
    paths = [c.heading_path for c in chunks]
    assert paths == [
        ("个人授信管理办法",),
        ("个人授信管理办法", "第一章 总则"),
        ("个人授信管理办法", "第二章 额度", "第一节 计算"),
        ("个人授信管理办法", "第三章 附则"),
    ]
    for c in chunks:
        # 原文位置必须精确：引用靠它定位
        assert BODY[c.start : c.end] == c.text
        assert c.end > c.start
        lines = BODY.split("\n")
        assert lines[c.line_start - 1] == c.text.split("\n")[0]
        assert lines[c.line_end - 1] == c.text.split("\n")[-1]
        assert c.chunk_id.startswith("src_wiki:credit#")
    # 代码块里的 # 不当作标题，留在第一章的块里
    assert "这不是标题" in chunks[1].text
    # “第二章 额度”只有下级标题、没有正文，不单独成块
    assert all(c.heading_path[-1] != "第二章 额度" for c in chunks)


def test_ids_stable_and_local_to_changes() -> None:
    before = {c.heading_path: c.chunk_id for c in chunk("src_wiki", "credit", BODY)}
    edited = BODY.replace("第一条 为规范", "第一条（修订）为规范")
    after = {c.heading_path: c.chunk_id for c in chunk("src_wiki", "credit", edited)}
    changed = [p for p in before if before[p] != after[p]]
    assert changed == [("个人授信管理办法", "第一章 总则")]
    # 只差空白与全半角的文本得到同一 ID
    spaced = BODY.replace("授信额度 = 月收入", "授信额度　=　月收入")
    assert (
        chunk("src_wiki", "credit", spaced)[2].chunk_id
        == chunk("src_wiki", "credit", BODY)[2].chunk_id
    )


def test_duplicate_text_gets_suffix() -> None:
    body = "## 甲\n\n免责声明。\n\n## 甲\n\n免责声明。\n"
    ids = [c.chunk_id for c in chunk("s", "d", body)]
    assert len(set(ids)) == 2 and ids[1].endswith("~2")


def test_long_section_split_on_paragraphs() -> None:
    para = "授信" * 150  # 300 字
    body = "## 长节\n\n" + "\n\n".join([para] * 5) + "\n"
    chunks = chunk("s", "d", body)
    assert len(chunks) > 1
    for c in chunks:
        assert body[c.start : c.end] == c.text
        assert len(c.text) <= MAX_CHARS + 10
    assert "".join(c.text.replace("\n", "") for c in chunks).count("授信") == 750


def test_wiki_chunk_ids_unchanged() -> None:
    """评测集按 chunk_id 引用 Wiki 样例；分块器重构后这些 ID 必须不变。"""
    wiki = Path(__file__).resolve().parents[4] / "tools" / "test-upstream" / "wiki"
    raw = (wiki / "credit-personal-loan.md").read_text(encoding="utf-8")
    body = raw[raw.index("\n---\n", 4) + 5 :]
    ids = {c.chunk_id for c in chunk("src_wikidemo", "credit-personal-loan", body)}
    assert {
        "src_wikidemo:credit-personal-loan#2565d11ea33f8145",
        "src_wikidemo:credit-personal-loan#805000b6ca427f8e",
        "src_wikidemo:credit-personal-loan#b0f6ac70f1d88a1f",
    } <= ids


def test_paragraphs_grouped_by_path_and_page() -> None:
    blocks = [
        Block("heading", "一、范围", ("通知", "一、范围"), 1, (72, 100, 200, 114)),
        Block("paragraph", "第一段。", ("通知", "一、范围"), 1, (72, 120, 500, 140)),
        Block("paragraph", "第二段。", ("通知", "一、范围"), 1, (72, 150, 480, 170)),
        # 换页：同一节在下一页继续，另成一块，页码与区域各自准确
        Block("paragraph", "第三段。", ("通知", "一、范围"), 2, (72, 90, 300, 110)),
        Block("heading", "二、附则", ("通知", "二、附则"), 2, (72, 130, 200, 144)),
    ]
    parsed = assemble(blocks)
    chunks = chunk_parsed("s", "d", parsed)
    # 只有标题、没有正文的“二、附则”不成块
    assert [(c.text, c.page, c.bbox) for c in chunks] == [
        ("一、范围\n\n第一段。\n\n第二段。", 1, (72, 100, 500, 170)),
        ("第三段。", 2, (72, 90, 300, 110)),
    ]
    for c in chunks:
        assert parsed.text[c.start : c.end] == c.text


def test_large_table_split_repeats_header() -> None:
    rows = table_text(
        [["序号", "制度条款", "说明"]] + [[str(i), f"条款{i}", "说明" * 20] for i in range(60)]
    )
    boxes = tuple((72.0, 100.0 + 10 * i, 520.0, 110.0 + 10 * i) for i in range(len(rows)))
    table = Block("table", "\n".join(rows), ("表",), 3, (72, 100, 520, 710), rows, boxes)
    parsed = assemble([table])
    assert len(table.text) > TABLE_MAX_CHARS
    chunks = chunk_parsed("s", "d", parsed)
    # 另有一块文字摘要，span 指向整张表
    summary = chunks.pop()
    assert summary.text == (
        "表格摘要：表中的表格，共 60 行，列为序号、制度条款、说明。"
        "序号包括0、1、2、3、4、5、6、7、8、9等 60 项。"
    )
    assert (summary.start, summary.end, summary.page) == (0, len(parsed.text), 3)
    assert len(chunks) > 1
    seen: list[str] = []
    for c in chunks:
        header, body = c.text.split("\n", 1)
        assert header == rows[0], "每块重复表头"
        # span 指向本组的行，原文中这一段就是块里表头之后的部分
        assert parsed.text[c.start : c.end] == body
        assert len(c.text) <= MAX_CHARS and len(body) >= len(header)
        first, last = rows.index(body.split("\n")[0]), rows.index(body.split("\n")[-1])
        assert c.bbox == (72.0, boxes[first][1], 520.0, boxes[last][3]) and c.page == 3
        seen += body.split("\n")
    assert seen == list(rows[1:]), "行不重不漏"
