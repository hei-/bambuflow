"""租户 pipeline 与全局 routing（governance 详设 3.1）：
从配置快照来，随快照整份替换（experiments.refresh）。

pipeline 给租户规则、回答阈值、单跳 LLM 重排开关与 agent 预算，routing 给生成模型与置信度权重，
prompt 给回答模板；没有生效版本时用代码里的默认值。
请求开始时取一次（Settings），进行中的请求不受快照切换影响（governance 详设 3.3）。

A/B 实验组（详设 12.3）另算一份 Settings：实验替换的 pipeline、routing、回答模板版本盖过生效的
（含灰度），阈值再盖过 pipeline 的；检索计划经 plan_version 告诉 retrieval。

灰度（governance 详设 3.2）：快照里的候选版本只对灰度租户里分到的用户生效，
分流规则与 retrieval 相同（in_canary），两边对同一用户得出同一结论。
用了候选版本时 why 里的 pipeline_version、routing_version、prompt_version 就是候选版本。

评测副本上配置发布的门禁另指定一个候选（candidate，governance 详设 4.1 第 ③④ 步），
对这次请求直接生效、不按用户分流；pipeline 的候选只对它的租户。
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from bambuflow.config.v1 import config_pb2
from bambuflow_orchestrator import confidence, prompt


@dataclass(frozen=True)
class Settings:
    """一次请求用的配置。None 表示用默认值。"""

    # 租户规则（提示词第 2 区，pipeline 的 tenant_rules）
    # 与经 governance 发布的回答模板（按 answer、agent-answer）
    tenant_rules: str = ""
    templates: Mapping[str, prompt.Template] = field(default_factory=dict)
    support_threshold: float | None = None
    min_coverage: float | None = None
    single_hop_llm_rerank: bool = False
    # agent 预算：多跳的最多轮数与每轮最多子问题数（None 用 agent 模块的上限）
    agent_max_rounds: int | None = None
    agent_max_sub_questions: int | None = None
    weights: confidence.Weights = field(default=confidence.DEFAULT)
    # 回答生成用的模型（routing）；空为部署的默认模型
    generation_model: str = ""
    # 写进 why：回放时凭它还原（为空表示没有生效版本、用了默认值）
    pipeline_version: str = ""
    routing_version: str = ""
    # 要 retrieval 用的检索计划版本（SearchRequest.plan_version）：只在实验组替换了 pipeline 时设，
    # 其余情况 retrieval 按同样的快照与分流规则自己选
    plan_version: str = ""
    # 提问人落在哪些灰度里（含 policy：候选包在 retrieval 生效），
    # 写进 why.canary_releases 供灰度观察分组
    canary_releases: tuple[str, ...] = ()


DEFAULT = Settings()


def weights_of(routing: config_pb2.RoutingConfig | None) -> confidence.Weights:
    if routing is None or not routing.HasField("confidence"):
        return confidence.DEFAULT
    c = routing.confidence
    return confidence.Weights(
        c.version, c.coverage, c.support, c.rerank, c.diversity, c.conflict, c.bias
    )


def in_canary(release_id: str, user: str, percent: int) -> bool:
    """sha256(release_id + "\\x00" + 用户) 的前 8 字节取模 100 小于 percent。
    与 retrieval grants.InCanary 相同。"""
    digest = hashlib.sha256(f"{release_id}\x00{user}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % 100 < percent


def template_of(t: config_pb2.PromptTemplate) -> prompt.Template:
    # 发布的模板版本号写成“模板@配置版本”：同一个配置版本只对应一份内容，回放时据此还原
    return prompt.Template(f"{t.scope}@{t.version_id}", t.system, t.user)


class Plans:
    def __init__(
        self,
        pipelines: Sequence[config_pb2.PipelineConfig] = (),
        routing: config_pb2.RoutingConfig | None = None,
        prompts: Sequence[config_pb2.PromptTemplate] = (),
        canaries: Sequence[config_pb2.Canary] = (),
    ) -> None:
        self._by_tenant: dict[str, config_pb2.PipelineConfig] = {}
        self._routing: config_pb2.RoutingConfig | None = None
        self._templates: dict[str, prompt.Template] = {}
        self._canaries: list[config_pb2.Canary] = []
        self.replace(pipelines, routing, prompts, canaries)

    def replace(
        self,
        pipelines: Sequence[config_pb2.PipelineConfig],
        routing: config_pb2.RoutingConfig | None,
        prompts: Sequence[config_pb2.PromptTemplate] = (),
        canaries: Sequence[config_pb2.Canary] = (),
    ) -> None:
        self._by_tenant = {p.tenant: p for p in pipelines}
        self._routing = routing
        self._templates = {t.scope: template_of(t) for t in prompts}
        self._canaries = list(canaries)

    def settings(
        self,
        tenant: str,
        user: str = "",
        candidate: config_pb2.Canary | None = None,
        experiment: config_pb2.RunningExperiment | None = None,
    ) -> Settings:
        """experiment 是提问人分到实验组的运行中实验（对照组与没有实验时为 None）。
        评测指定的候选最后生效：门禁评的是候选本身。"""
        p = self._by_tenant.get(tenant)
        r = self._routing
        templates = dict(self._templates)
        releases: list[str] = []
        picked: list[config_pb2.Canary] = []
        for c in self._canaries:
            if c.tenant != tenant or not in_canary(c.release_id, user, c.percent):
                continue
            releases.append(c.release_id)
            picked.append(c)
        plan_version = ""
        if experiment is not None:
            picked.extend(_variants(experiment))
            if experiment.HasField("pipeline"):
                plan_version = experiment.pipeline.version_id
        if candidate is not None and candidate.tenant in ("", tenant):
            picked.append(candidate)
        for c in picked:
            kind = c.WhichOneof("candidate")
            if kind == "pipeline":
                p = c.pipeline
            elif kind == "routing":
                r = c.routing
            elif kind == "prompt":
                templates[c.prompt.scope] = template_of(c.prompt)
        a = p.answer if p is not None else config_pb2.AnswerPlan()
        support = a.support_threshold if a.HasField("support_threshold") else None
        coverage = a.min_coverage if a.HasField("min_coverage") else None
        if experiment is not None:
            if experiment.HasField("support_threshold"):
                support = experiment.support_threshold
            if experiment.HasField("min_coverage"):
                coverage = experiment.min_coverage
        return Settings(
            tenant_rules=a.tenant_rules,
            templates=templates,
            support_threshold=support,
            min_coverage=coverage,
            single_hop_llm_rerank=a.single_hop_llm_rerank,
            agent_max_rounds=a.agent_max_rounds if a.HasField("agent_max_rounds") else None,
            agent_max_sub_questions=(
                a.agent_max_sub_questions if a.HasField("agent_max_sub_questions") else None
            ),
            weights=weights_of(r),
            generation_model=r.generation_model if r is not None else "",
            pipeline_version=p.version_id if p is not None else "",
            routing_version=r.version_id if r is not None else "",
            plan_version=plan_version,
            canary_releases=tuple(releases),
        )


def _variants(exp: config_pb2.RunningExperiment) -> list[config_pb2.Canary]:
    """实验替换的配置装成和灰度候选一样的形状，按同一段代码套用。"""
    out: list[config_pb2.Canary] = []
    if exp.HasField("pipeline"):
        out.append(config_pb2.Canary(pipeline=exp.pipeline))
    if exp.HasField("routing"):
        out.append(config_pb2.Canary(routing=exp.routing))
    out.extend(config_pb2.Canary(prompt=t) for t in exp.prompts)
    return out
