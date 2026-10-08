"""生成 Wiki 样例的干扰页面（P1 计划 T1b、待确认 P1-6 的 A），只用于开发与评测。

干扰页面与评测集考查的制度主题相近（分行实施细则、例会纪要），让检索的前 10 名真正有竞争；
它们只引用总行制度而不改其中的数值，所以不会让评测集的标准答案变得有争议。
内容由固定的组合确定地生成，不进仓库：构建 connectors 开发镜像时运行
（python gen_distractors.py <输出目录>），e2e 用 pages() 得到页面清单。
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

BRANCHES = [
    "北京",
    "上海",
    "广州",
    "深圳",
    "杭州",
    "南京",
    "苏州",
    "成都",
    "重庆",
    "武汉",
    "西安",
    "天津",
    "长沙",
    "郑州",
    "济南",
    "青岛",
    "宁波",
    "厦门",
    "福州",
    "合肥",
    "南昌",
    "昆明",
    "贵阳",
    "南宁",
    "沈阳",
    "大连",
    "哈尔滨",
    "长春",
    "石家庄",
    "太原",
]


@dataclass(frozen=True)
class Topic:
    key: str
    policy: str  # 被引用的总行制度
    focus: list[str]  # 分行补充规定关注的环节（不含数值）
    kind: str  # credit / general / risk，决定 ACL 与密级


TOPICS = [
    Topic(
        "consumer",
        "个人消费贷款管理办法",
        ["额度测算资料清单", "审批流转顺序", "利率定价沟通口径"],
        "credit",
    ),
    Topic(
        "mortgage",
        "个人住房贷款管理办法",
        ["首付款来源核查", "期限与还款方式说明", "提前还款受理流程"],
        "credit",
    ),
    Topic(
        "collateral",
        "抵质押物管理规程",
        ["评估机构对接方式", "押品重估提醒", "权证入库交接"],
        "credit",
    ),
    Topic(
        "postloan",
        "贷后管理办法",
        ["贷后检查记录模板", "预警信息录入规范", "催收话术要求"],
        "credit",
    ),
    Topic(
        "smallbiz",
        "小微企业贷款操作规程",
        ["准入资料核验", "调查报告撰写要求", "受托支付材料"],
        "credit",
    ),
    Topic(
        "travel", "差旅费报销办法", ["出差审批单填写", "票据粘贴要求", "报销系统操作"], "general"
    ),
    Topic(
        "training",
        "员工培训管理办法",
        ["培训报名方式", "学时登记方法", "外部培训申请材料"],
        "general",
    ),
    Topic(
        "security",
        "信息系统密码策略",
        ["密码重置渠道", "设备登记流程", "可疑邮件识别要点"],
        "general",
    ),
    Topic(
        "marketrisk",
        "市场风险限额管理办法",
        ["限额报表报送时点", "超限沟通机制", "交易台日终核对"],
        "risk",
    ),
    Topic(
        "oprisk",
        "操作风险事件报告办法",
        ["事件登记字段说明", "整改台账维护", "部门联络人职责"],
        "risk",
    ),
]
DEPTS = ["信贷管理部", "运营管理部", "综合管理部"]
ACCESS = {
    "credit": ("internal", "[group:credit-l2]", "alice"),
    "general": ("public", "[group:all-staff]", "eve"),
    "risk": ("confidential", "[group:risk-dept]", "bob"),
}


@dataclass(frozen=True)
class Page:
    id: str
    markdown: str


def _page(pid: str, title: str, kind: str, day: int, sections: list[tuple[str, str]]) -> Page:
    classification, acl, owner = ACCESS[kind]
    body = "\n\n".join(f"## {h}\n\n{t}" for h, t in sections)
    updated = f"2025-{(day % 9) + 1:02d}-{(day % 27) + 1:02d}T00:00:00Z"
    md = (
        f"---\nid: {pid}\ntitle: {title}\nowner: {owner}\n"
        f"classification: {classification}\nacl: {acl}\nupdated_at: {updated}\n---\n"
        f"# {title}\n\n{body}\n"
    )
    return Page(pid, md)


def pages() -> list[Page]:
    out: list[Page] = []
    for b, branch in enumerate(BRANCHES):
        for t, topic in enumerate(TOPICS):
            # 每个分行只覆盖一部分主题，页面数量适中且分布不均，更接近真实知识库
            if (b + t) % 3 == 2:
                continue
            a, c, d = topic.focus
            out.append(
                _page(
                    f"dist-{topic.kind}-{topic.key}-{b:02d}",
                    f"{branch}分行关于执行《{topic.policy}》的补充通知",
                    topic.kind,
                    b + t,
                    [
                        (
                            "总体要求",
                            f"各支行办理相关业务时，额度、比例、期限等标准一律按总行《{topic.policy}》执行，"
                            f"本通知只对{branch}分行辖内的操作环节作补充。",
                        ),
                        (
                            a,
                            f"{a}由{branch}分行{DEPTS[b % 3]}统一解释，"
                            f"支行在执行中遇到疑问的，通过分行工作群提交，不另行发文。",
                        ),
                        (
                            c,
                            f"{c}以分行模板为准，模板更新后在分行门户首页公告。"
                            f"{d}按总行制度办理，分行不作额外要求。",
                        ),
                    ],
                )
            )
        # 每个分行一期例会纪要，涉及多个主题，是典型的“提到了但不回答问题”的干扰
        mentioned = [TOPICS[(b + k) % len(TOPICS)] for k in range(3)]
        kind = "general" if all(m.kind == "general" for m in mentioned) else "credit"
        out.append(
            _page(
                f"dist-minutes-{b:02d}",
                f"{branch}分行第 {b + 3} 期业务例会纪要",
                kind,
                b,
                [
                    ("参会人员", f"{branch}分行各部门负责人及支行行长参加，会议由分管副行长主持。"),
                    (
                        "讨论事项",
                        "；".join(f"学习总行《{m.policy}》，讨论{m.focus[0]}" for m in mentioned)
                        + "。",
                    ),
                    (
                        "会议决定",
                        "各项业务继续按总行现行制度执行，分行层面不调整任何标准；相关部门两周内反馈执行情况。",
                    ),
                ],
            )
        )
    return out


def main() -> None:
    target = Path(sys.argv[1])
    target.mkdir(parents=True, exist_ok=True)
    for p in pages():
        (target / f"{p.id}.md").write_text(p.markdown, encoding="utf-8")
    print(f"生成干扰页面 {len(pages())} 页 → {target}", file=sys.stderr)


if __name__ == "__main__":
    main()
