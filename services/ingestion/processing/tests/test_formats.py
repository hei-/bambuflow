"""各格式解析器的 golden 文件测试（入库管道详设第 13 节）。

样例是共享盘样例源 tools/test-upstream/fileshare 里的文件。

验证三件事：解析出的结构（标题路径、表头、去掉的页眉页脚与引用历史）；每块的 span 能回指原文
（字符区间回指解析后的全文，页码与区域回指原 PDF 的页面）；OCR 路径的输入与输出（用假的 OCR 引擎）。
"""

import io
from pathlib import Path

import pdfplumber
import pytest
from bambuflow_processing.blocks import Block, assemble
from bambuflow_processing.chunker import Chunk, chunk_parsed
from bambuflow_processing.ocr import OcrLine
from bambuflow_processing.parsers import DOCX, XLSX, parse
from bambuflow_processing.pdf import OCR_ZOOM, OcrRequiredError
from PIL import Image

SAMPLES = Path(__file__).resolve().parents[4] / "tools" / "test-upstream" / "fileshare"


def sample(name: str) -> bytes:
    return (SAMPLES / name).read_bytes()


def chunks_of(content_type: str, name: str, ocr: "FakeOcr | None" = None) -> list[Chunk]:
    parsed = assemble(parse(content_type, sample(name), "标题", ocr))
    chunks = chunk_parsed("src_fs", "doc", parsed)
    for c in chunks:
        # 字符区间回指解析后的全文：不切分的块正文就是全文的这一段
        assert parsed.text[c.start : c.end] in c.text, c.chunk_id
        assert c.chunk_id.startswith("src_fs:doc#") and c.line_start <= c.line_end
    return chunks


def squash(s: str) -> str:
    return "".join(s.replace("|", " ").split())


class FakeOcr:
    """按调用顺序返回预先给定的各页识别结果，并记下收到的图片。"""

    def __init__(self, pages: list[list[OcrLine]]) -> None:
        self.pages = pages
        self.images: list[bytes] = []

    def recognize(self, image: bytes) -> list[OcrLine]:
        self.images.append(image)
        return self.pages[len(self.images) - 1]


def line(text: str, x0: float, y0: float, x1: float, y1: float, score: float = 0.98) -> OcrLine:
    return OcrLine(text, (x0, y0, x1, y1), score)


# ---- 文本 PDF ----


def test_text_pdf_structure_and_spans() -> None:
    chunks = chunks_of("application/pdf", "credit-rate-notice-2026q4.pdf")
    title = "关于 2026 年第四季度个人贷款利率定价的通知"
    paths = {c.heading_path for c in chunks}
    assert (title, "一、适用范围") in paths and (title, "三、优惠定价审批") in paths
    text = "\n".join(c.text for c in chunks)
    # 页眉页脚去重：重复的页眉与页码行不入库
    assert "信贷管理部发文" not in text and "第 1 页" not in text
    tables = [c for c in chunks if c.text.startswith("| 产品 |")]
    assert len(tables) == 1 and "| 个人经营贷款 | 3 年以内（含） | LPR + 0.80% |" in tables[0].text
    assert {c.page for c in chunks} == {1, 2}
    assert [c.page for c in chunks if "优惠幅度" in c.text] == [2]
    with pdfplumber.open(SAMPLES / "credit-rate-notice-2026q4.pdf") as doc:
        for c in chunks:
            # 页码与区域回指原文：按区域从原页面取出的文字包含块的正文
            assert c.page is not None and c.bbox is not None
            x0, y0, x1, y1 = c.bbox
            region = doc.pages[c.page - 1].within_bbox((x0 - 1, y0 - 1, x1 + 1, y1 + 1))
            assert squash(c.text) in squash(region.extract_text()), c.text


# ---- 扫描件 PDF（OCR） ----

# 渲染倍数为 2：页面坐标 (x, y) 点对应像素 (2x, 2y)
SCAN_PAGE_1 = [
    line("运营管理部·公司内部", 144, 60, 400, 80),  # 页眉，在上边距内
    line("关于开展消防安全检查的通知", 144, 200, 900, 240),  # 行高 40：标题
    line("一、检查时间", 144, 280, 400, 310),  # 行高 30：二级标题
    line("2026 年 11 月 1 日至 11 月 20 日，各营业网点先自查，", 144, 330, 1000, 352),
    line("再由运营管理部抽查。", 144, 362, 500, 384),
    line("模糊的污渍", 144, 600, 300, 620, score=0.2),  # 置信度太低，丢弃
    line("第 1 页 共 2 页", 144, 1620, 400, 1640),  # 页脚
]
SCAN_PAGE_2 = [
    line("运营管理部·公司内部", 144, 60, 400, 80),
    line("三、整改要求", 144, 200, 400, 230),
    line("检查发现的问题须在 5 个工作日内整改完毕。", 144, 250, 1000, 272),
    line("第 2 页 共 2 页", 144, 1620, 400, 1640),
]


def test_scanned_pdf_goes_through_ocr() -> None:
    ocr = FakeOcr([SCAN_PAGE_1, SCAN_PAGE_2])
    chunks = chunks_of("application/pdf", "branch-fire-inspection-scan.pdf", ocr)
    # 每个扫描页渲染成一张 PNG 交给 OCR，尺寸为页面的 2 倍
    assert len(ocr.images) == 2
    for img in ocr.images:
        assert img.startswith(b"\x89PNG")
        with Image.open(io.BytesIO(img)) as pix:
            assert pix.size == (round(595 * OCR_ZOOM), round(842 * OCR_ZOOM))
    assert [(c.heading_path, c.page) for c in chunks] == [
        (("关于开展消防安全检查的通知", "一、检查时间"), 1),
        (("关于开展消防安全检查的通知", "三、整改要求"), 2),
    ]
    first = chunks[0]
    assert first.text == (
        "一、检查时间\n\n2026 年 11 月 1 日至 11 月 20 日，各营业网点先自查，再由运营管理部抽查。"
    )
    # 像素坐标换算回页面坐标（点）
    assert first.bbox == (72.0, 140.0, 500.0, 192.0)
    text = "\n".join(c.text for c in chunks)
    assert "污渍" not in text and "公司内部" not in text and "第 1 页" not in text


def test_scanned_pdf_without_ocr_is_rejected() -> None:
    with pytest.raises(OcrRequiredError):
        parse("application/pdf", sample("branch-fire-inspection-scan.pdf"), "x", None)


# ---- Office ----


def test_docx_headings_and_table() -> None:
    chunks = chunks_of(DOCX, "it-asset-management.docx")
    title = "办公设备与软件资产管理办法"
    table = [c for c in chunks if c.text.startswith("| 设备类别 |")]
    assert len(table) == 1 and table[0].heading_path == (title, "第二章 设备配发与报废")
    assert "| 笔记本电脑 | 14 英寸，16 GB 内存 | 4 年 | 部门负责人 |" in table[0].text
    assert any("10 个工作日内完成安全评估" in c.text for c in chunks)
    assert all(c.page is None and c.bbox is None for c in chunks)


def test_xlsx_sheets_keep_header_and_percent_format() -> None:
    chunks = chunks_of(XLSX, "risk-limit-monitor-2026q3.xlsx")
    by_sheet = {c.heading_path[-1]: c.text for c in chunks}
    assert set(by_sheet) == {"限额监控", "预警处置"}
    monitor = by_sheet["限额监控"].split("\n")
    assert monitor[0] == "| 限额指标 | 限额值 | 9 月末实际值 | 使用率 | 状态 |"
    assert "| 前十大客户授信集中度 | 50% | 46.5% | 93.0% | 预警 |" in monitor


# ---- 邮件 ----


def test_eml_strips_quotes_signature_and_keeps_attachment() -> None:
    chunks = chunks_of("message/rfc822", "aml-training-notice.eml")
    subject = "关于 2026 年反洗钱专项培训安排的通知"
    text = "\n".join(c.text for c in chunks)
    assert "考试合格线为 80 分" in text
    assert "70 分" not in text, "引用的历史邮件不入库"
    assert "分机" not in text and "免责声明" not in text, "签名与免责声明不入库"
    assert "@" not in text, "只保留显示名，不入库邮件地址"
    assert "发件人：合规部" in text and "日期：2026-10-09" in text
    attachment = [c for c in chunks if c.heading_path == (subject, "附件：培训场次安排.csv")]
    assert len(attachment) == 1
    assert attachment[0].text.startswith("| 场次 | 日期 | 时间 | 形式 |\n| 第一场 |")


# ---- 图片 ----


def test_image_is_one_chunk_with_region() -> None:
    ocr = FakeOcr(
        [
            [
                line("访客登记须知", 290, 130, 720, 200),
                line("一、访客须在前台出示有效身份证件并登记。", 290, 260, 1150, 300),
                line("二、访客证当日有效。", 290, 380, 800, 420),
            ]
        ]
    )
    chunks = chunks_of("image/png", "visitor-notice.png", ocr)
    assert len(ocr.images) == 1 and ocr.images[0] == sample("visitor-notice.png")
    assert len(chunks) == 1
    c = chunks[0]
    assert c.text == "访客登记须知\n一、访客须在前台出示有效身份证件并登记。\n二、访客证当日有效。"
    assert c.page is None and c.bbox == (290, 130, 1150, 420)


class FakeDescriber:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls = 0

    def describe(self, image: bytes) -> str:
        self.calls += 1
        return self.text


def text_pdf_with_image() -> bytes:
    """手写的一页 PDF（400 × 400）：有文字层，(50, 100)–(170, 180) 处嵌一张 120 × 80 的图，
    (300, 300) 处另有一张 30 × 30 的图标。"""
    content = (
        b"BT /F1 12 Tf 20 360 Td (Loan rate notice for the fourth quarter.) Tj ET\n"
        b"q 120 0 0 80 50 100 cm /Im1 Do Q\nq 30 0 0 30 300 300 cm /Im2 Do Q\n"
    )

    def image(w: int, h: int) -> bytes:
        head = f"<< /Type /XObject /Subtype /Image /Width {w} /Height {h} /ColorSpace /DeviceGray"
        head += f" /BitsPerComponent 8 /Length {w * h} >>\nstream\n"
        return head.encode() + b"\xff" * (w * h) + b"\nendstream"

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 400] /Contents 4 0 R /Resources"
        b" << /Font << /F1 5 0 R >> /XObject << /Im1 6 0 R /Im2 7 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n" % len(content) + content + b"endstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        image(120, 80),
        image(30, 30),
    ]
    out = b"%PDF-1.7\n"
    offsets = []
    for n, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % n + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    out += b"".join(b"%010d 00000 n \n" % o for o in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (
        len(objects) + 1,
        xref,
    )
    return out


def test_embedded_image_in_text_pdf_becomes_image_chunk() -> None:
    data = text_pdf_with_image()
    # 没配 OCR 与描述：图片只是补充，正文照常入库
    assert [b.kind for b in parse("application/pdf", data, "利率通知", None)] == ["paragraph"]
    ocr = FakeOcr([[line("2026 年第四季度利率表", 10, 10, 110, 30)]])
    describe = FakeDescriber("柱状图：各期限贷款利率对比")
    blocks = parse("application/pdf", data, "利率通知", ocr, describe)
    images = [b for b in blocks if b.kind == "image"]
    # 图标（30 × 30）跳过；图片块正文是描述加 OCR 文字，区域是图片在页面上的位置（原点在左上角）
    assert len(images) == 1 and describe.calls == 1 and len(ocr.images) == 1
    assert images[0].text == "图片说明：柱状图：各期限贷款利率对比\n2026 年第四季度利率表"
    assert images[0].page == 1 and images[0].bbox == (50, 220, 170, 300)


def test_image_description_goes_before_ocr_text() -> None:
    ocr = FakeOcr([[line("访客登记须知", 290, 130, 720, 200)]])
    blocks = parse(
        "image/png", sample("visitor-notice.png"), "x", ocr, FakeDescriber("前台张贴的告示")
    )
    assert blocks[0].text == "图片说明：前台张贴的告示\n访客登记须知"


def test_tiny_images_are_skipped() -> None:
    buf = io.BytesIO()
    Image.new("L", (30, 30), 255).save(buf, format="PNG")
    icon = buf.getvalue()
    assert parse("image/png", icon, "x", FakeOcr([])) == []


def test_unsupported_and_broken_files() -> None:
    from bambuflow_processing.blocks import ParseError

    for ctype, data in [
        ("application/zip", b"PK"),
        ("application/pdf", b"not a pdf"),
        (DOCX, b"not a docx"),
        (XLSX, b"not a xlsx"),
    ]:
        with pytest.raises(ParseError):
            parse(ctype, data, "x", None)


def test_paged_block_without_region_rejects_whole_document() -> None:
    from bambuflow_processing.chunker import MissingSpanError

    good = Block("paragraph", "第一段。", ("标题",), 1, (0, 0, 10, 10))
    bad = Block("paragraph", "第二段。", ("标题",), 2, None)
    with pytest.raises(MissingSpanError):
        chunk_parsed("src_fs", "doc", assemble([good, bad]))
