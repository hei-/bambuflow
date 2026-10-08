"""生成系统架构全貌图 docs/diagrams/architecture.svg（分层排版，手工定坐标，避免自动布局的交叉线）。

    python3 docs/diagrams/architecture.py            # 写 architecture.svg
PNG 由浏览器截图得到（README 引用 PNG，任何环境都能显示），见文件末尾的说明。
改内容只改下面的数据；坐标单位为像素，画布 1400 × 880。
"""

from __future__ import annotations

from html import escape
from pathlib import Path

W, H = 1400, 880
FONT = "PingFang SC, Microsoft YaHei, Noto Sans CJK SC, Source Han Sans SC, sans-serif"

# 各平面的配色：（底色，边框，强调色）
BLUE = ("#eef4ff", "#a9c1f5", "#2f5fcb")
PURPLE = ("#f4f0ff", "#c7b5f7", "#6a3fd8")
GREEN = ("#ebf8f0", "#9fd8b4", "#23854b")
AMBER = ("#fff8e8", "#ecd28f", "#a77a06")
GRAY = ("#f5f6f8", "#c8cdd6", "#5b6474")
NAVY = "#1f2d4d"

out: list[str] = []


def text(x: float, y: float, s: str, size: float = 13, color: str = "#334155", weight: int = 400,
         anchor: str = "middle") -> None:
    out.append(
        f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" font-weight="{weight}" '
        f'text-anchor="{anchor}">{escape(s)}</text>'
    )


def plane(x: float, y: float, w: float, h: float, title: str, colors: tuple[str, str, str],
          dashed: bool = False) -> None:
    fill, stroke, accent = colors
    dash = ' stroke-dasharray="6 5"' if dashed else ""
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" '
               f'stroke="{stroke}" stroke-width="1.5"{dash}/>')
    text(x + 18, y + 26, title, 15, accent, 700, "start")


def box(x: float, y: float, w: float, h: float, title: str, lines: list[str],
        colors: tuple[str, str, str], dashed: bool = False) -> None:
    _, stroke, accent = colors
    dash = ' stroke-dasharray="5 4"' if dashed else ""
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#ffffff" '
               f'stroke="{stroke}" stroke-width="1.5" filter="url(#shadow)"{dash}/>')
    out.append(f'<rect x="{x}" y="{y}" width="5" height="{h}" rx="2.5" fill="{accent}"/>')
    cx = x + w / 2 + 2
    top = y + h / 2 - (len(lines) * 18) / 2 + 4
    text(cx, top, title, 15, NAVY, 700)
    for i, line in enumerate(lines):
        text(cx, top + 21 + i * 18, line, 12.5, "#5b6474")


def store(x: float, y: float, w: float, h: float, title: str, sub: str) -> None:
    _, stroke, accent = AMBER
    ry = 10
    out.append(f'<path d="M{x},{y + ry} a{w / 2},{ry} 0 0,0 {w},0 v{h - 2 * ry} a{w / 2},{ry} 0 0,1 {-w},0 z" '
               f'fill="#ffffff" stroke="{accent}" stroke-width="1.5" filter="url(#shadow)"/>')
    out.append(f'<ellipse cx="{x + w / 2}" cy="{y + ry}" rx="{w / 2}" ry="{ry}" fill="#fff3cf" '
               f'stroke="{accent}" stroke-width="1.5"/>')
    text(x + w / 2, y + h / 2 + 8, title, 14.5, NAVY, 700)
    text(x + w / 2, y + h / 2 + 27, sub, 12, "#5b6474")


def arrow(points: list[tuple[float, float]], label: str = "", color: str = "#64748b",
          dashed: bool = False, label_at: tuple[float, float] | None = None, bold: bool = False) -> None:
    d = "M" + " L".join(f"{x},{y}" for x, y in points)
    dash = ' stroke-dasharray="6 4"' if dashed else ""
    width = 2.4 if bold else 1.6
    mid = color.lstrip("#")
    out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{dash} '
               f'marker-end="url(#a{mid})"/>')
    if label:
        lx, ly = label_at or points[len(points) // 2]
        pad = 6
        wlen = len(label) * 12.5 + pad * 2
        out.append(f'<rect x="{lx - wlen / 2}" y="{ly - 11}" width="{wlen}" height="20" rx="10" '
                   f'fill="#ffffff" stroke="{color}" stroke-opacity="0.35"/>')
        text(lx, ly + 4, label, 12, color, 600)


COLORS = ["#64748b", BLUE[2], PURPLE[2], GREEN[2], AMBER[2]]

# ---------- 画面 ----------
out.append(f'<rect width="{W}" height="{H}" fill="#ffffff"/>')
text(40, 44, "bambuflow · 企业级 RAG 平台架构全貌", 22, NAVY, 800, "start")
text(40, 68, "三个平面只通过版本化索引与元数据交汇：只有 retrieval 读索引，只有 indexing 写索引；"
     "权限召回前生效、返回前复核，一律 fail-closed", 13, "#64748b", 400, "start")

# 主区 x 40–970，治理列 x 1010–1360
for i, (t, s2) in enumerate([("业务系统", "绑定租户"), ("console 等平台级客户端", "请求头选择租户"),
                             ("外部 agent", "MCP 工具")]):
    x = 56 + i * 310
    out.append(f'<rect x="{x}" y="92" width="280" height="46" rx="23" fill="#f1f5ff" stroke="#b9c8ef"/>')
    text(x + 140, 113, t, 14, NAVY, 700)
    text(x + 140, 130, s2, 11.5, "#64748b")

# 接入层
out.append(f'<rect x="40" y="168" width="930" height="78" rx="12" fill="{NAVY}"/>')
text(505, 198, "gateway  ·  唯一入口", 17, "#ffffff", 800)
text(505, 222, "认证与租户 · 签发 5 分钟内部 Principal · 限流与配额 · 入口审计（WORM）· 管理接口 RBAC · REST / SSE / MCP",
     12.5, "#c9d6f2")

# 在线服务平面
plane(40, 276, 930, 196, "在线服务平面", BLUE)
box(64, 316, 210, 132, "qa/batch", ["异步批量问答", "逐条换发 Principal"], BLUE)
box(296, 316, 320, 132, "orchestrator",
    ["意图路由 · 单跳 / 多步检索 / 清单", "生成 · 逐句校验（引文 / NLI / 权限）", "熔断 · A/B 分组 · sql_query"], BLUE)
box(638, 316, 308, 132, "retrieval",
    ["权限过滤下推 · 稠密 + BM25 + 图召回", "RRF 融合 · MMR 去重 · 交叉编码器精排", "返回前复核 · PII 掩码"], BLUE)

# 离线入库平面
plane(40, 500, 930, 168, "离线入库平面", GREEN)
box(64, 538, 260, 108, "connectors", ["Wiki · 共享盘 · 日志 · 表结构", "变更检测 · 删除保护"], GREEN)
box(346, 538, 290, 108, "processing", ["解析 / OCR · 分级脱敏 · 分块", "摘要 · 可疑指令标记 · 向量化"], GREEN)
box(658, 538, 288, 108, "indexing", ["唯一写入口", "版本发布与回滚（双人复核）"], GREEN)

# 存储
plane(40, 696, 930, 150, "存储", AMBER)
text(232, 722, "各模块自有 schema，不共享表", 12, AMBER[2], 400, "start")
store(80, 736, 230, 92, "Redis", "事件流 · 会话 · 计数")
store(360, 736, 260, 92, "对象存储（对象锁）", "原文 · 审计 WORM")
store(660, 736, 270, 92, "Postgres + pgvector", "版本化索引 · 元数据")

# 横切治理平面
plane(1010, 168, 350, 500, "横切治理平面", PURPLE)
box(1030, 210, 310, 128, "governance",
    ["配置快照 · 受控词表 · 实验登记", "数据源与授权 · 审计查询", "紧急拒绝（1 分钟内生效）"], PURPLE)
box(1030, 358, 310, 128, "evaluation",
    ["回放评测与门禁 · 权限评测集", "A/B 分析与护栏", "在线抽样评审"], PURPLE)
box(1030, 506, 310, 140, "webhook · 身份同步",
    ["平台事件签名投递", "LDAP / SCIM → 平台身份库"], PURPLE)

# 外部系统
plane(1010, 696, 350, 150, "外部系统（经端口接入）", GRAY, dashed=True)
for i, t in enumerate(["数据源", "模型服务", "LLM 网关", "数据平台", "Vault", "IdP / 目录"]):
    x = 1028 + (i % 3) * 108
    y = 738 + (i // 3) * 50
    out.append(f'<rect x="{x}" y="{y}" width="98" height="38" rx="8" fill="#ffffff" stroke="#c8cdd6" '
               f'stroke-dasharray="4 3"/>')
    text(x + 49, y + 24, t, 12.5, "#475569", 600)

# ---------- 连线 ----------
for i in range(3):
    arrow([(196 + i * 310, 138), (196 + i * 310, 166)], color=NAVY)
arrow([(169, 246), (169, 314)], "批次", BLUE[2], label_at=(169, 282))
arrow([(456, 246), (456, 314)], "问答", BLUE[2], label_at=(456, 282))
arrow([(792, 246), (792, 314)], "检索", BLUE[2], label_at=(792, 282))
arrow([(274, 382), (294, 382)], color=BLUE[2])
arrow([(616, 382), (636, 382)], color=BLUE[2])
arrow([(324, 592), (344, 592)], color=GREEN[2])
arrow([(636, 592), (656, 592)], color=GREEN[2])
arrow([(802, 646), (802, 734)], "只写", GREEN[2], label_at=(802, 690), bold=True)
arrow([(946, 430), (958, 430), (958, 800), (934, 800)], "只读", BLUE[2], label_at=(958, 690), bold=True)
arrow([(195, 646), (195, 734)], "变更事件", GREEN[2], label_at=(195, 690))
arrow([(970, 232), (1028, 232)], color=NAVY)
text(990, 264, "管理接口", 12, NAVY, 600)
arrow([(1030, 300), (985, 300), (985, 340), (948, 340)], color=PURPLE[2], dashed=True)
out.append('<rect x="845" y="289" width="112" height="20" rx="10" fill="#ffffff" stroke="#6a3fd8" stroke-opacity="0.35"/>')
text(901, 303, "配置与授权快照", 12, PURPLE[2], 600)

defs = "".join(
    f'<marker id="a{c.lstrip("#")}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
    f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>'
    for c in COLORS + [NAVY]
)
svg = (
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
    f'font-family="{FONT}"><defs>{defs}'
    '<filter id="shadow" x="-10%" y="-10%" width="120%" height="130%">'
    '<feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#1f2d4d" flood-opacity="0.08"/></filter>'
    f'</defs>{"".join(out)}</svg>\n'
)
Path(__file__).with_name("architecture.svg").write_text(svg, encoding="utf-8")
# PNG：用浏览器打开 architecture.svg 截图（2 倍像素），或 rsvg-convert -z 2 architecture.svg > architecture.png
