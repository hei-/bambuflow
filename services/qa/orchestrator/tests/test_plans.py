import asyncio

import pytest

from bambuflow.config.v1 import config_pb2
from bambuflow_orchestrator import confidence, experiments, plans
from bambuflow_orchestrator.experiments import Experiments


def pipeline(tenant: str = "credit") -> config_pb2.PipelineConfig:
    return config_pb2.PipelineConfig(
        tenant=tenant,
        version_id="cfv_p1",
        answer=config_pb2.AnswerPlan(
            support_threshold=0.6, min_coverage=0.7, single_hop_llm_rerank=True
        ),
    )


ROUTING = config_pb2.RoutingConfig(
    version_id="cfv_r1",
    confidence=config_pb2.ConfidenceWeights(
        version="confidence-abc",
        coverage=2.5,
        support=1.5,
        rerank=1,
        diversity=0.3,
        conflict=1,
        bias=-2.5,
    ),
)


def test_settings_from_pipeline_and_routing() -> None:
    s = plans.Plans([pipeline()], ROUTING).settings("credit")
    assert (s.support_threshold, s.min_coverage, s.single_hop_llm_rerank) == (0.6, 0.7, True)
    assert (s.pipeline_version, s.routing_version) == ("cfv_p1", "cfv_r1")
    assert s.weights.version == "confidence-abc" and s.weights.coverage == 2.5
    # 没有 pipeline 的租户与没有 routing 时用默认值
    other = plans.Plans([pipeline()]).settings("ops")
    assert other == plans.Settings() and other.weights is confidence.DEFAULT


def test_refresh_replaces_plans_from_snapshot() -> None:
    p = plans.Plans()
    calls = 0

    async def fetch() -> config_pb2.GetSnapshotResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            return config_pb2.GetSnapshotResponse(
                snapshot_version="v1", pipelines=[pipeline()], routing=ROUTING
            )
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(experiments.refresh(Experiments(), fetch, every=0, plans=p))
    assert p.settings("credit").pipeline_version == "cfv_p1"
    assert p.settings("credit").routing_version == "cfv_r1"


def test_published_templates_are_versioned_by_config_version() -> None:
    t = config_pb2.PromptTemplate(
        scope="answer", version_id="cfv_t1", system="S{{tenant_rules}}", user="U"
    )
    s = plans.Plans([pipeline()], None, [t]).settings("credit")
    assert s.templates["answer"].version == "answer@cfv_t1"
    assert "agent-answer" not in s.templates


def test_experiment_variant_overrides_active_and_canary() -> None:
    t = config_pb2.PromptTemplate(scope="answer", version_id="cfv_t1", system="S", user="U")
    variant_pipeline = config_pb2.PipelineConfig(
        tenant="credit",
        version_id="cfv_p2",
        answer=config_pb2.AnswerPlan(min_coverage=0.9, agent_max_rounds=2),
    )
    exp = config_pb2.RunningExperiment(
        exp_id="exp_1",
        tenant="credit",
        support_threshold=0.45,
        pipeline=variant_pipeline,
        routing=config_pb2.RoutingConfig(version_id="cfv_r2", generation_model="alt-model"),
        prompts=[
            config_pb2.PromptTemplate(scope="answer", version_id="cfv_t2", system="S", user="U")
        ],
    )
    p = plans.Plans([pipeline()], ROUTING, [t])
    base = p.settings("credit", "alice")
    s = p.settings("credit", "alice", experiment=exp)
    # 阈值在 pipeline 变体之上再替换；没设的沿用变体的
    assert (s.support_threshold, s.min_coverage, s.agent_max_rounds) == (0.45, 0.9, 2)
    assert (s.pipeline_version, s.plan_version, s.routing_version) == ("cfv_p2", "cfv_p2", "cfv_r2")
    assert s.generation_model == "alt-model" and s.templates["answer"].version == "answer@cfv_t2"
    # routing 变体没有权重：用默认权重，不混用旧 routing 的
    assert s.weights is confidence.DEFAULT
    assert base.plan_version == "" and base.generation_model == "" and base.agent_max_rounds is None
    # 只改阈值的实验不要求 retrieval 换计划
    only = config_pb2.RunningExperiment(exp_id="exp_1", tenant="credit", min_coverage=0.5)
    s = p.settings("credit", "alice", experiment=only)
    assert s.plan_version == "" and s.pipeline_version == "cfv_p1" and s.min_coverage == 0.5


def test_in_canary_matches_retrieval() -> None:
    # 与 retrieval grants.InCanary 的测试用同一组数：两边对同一用户必须得出同一个结论
    for release, user, bucket in (
        ("rel_a", "alice", 47),
        ("rel_a", "bob", 28),
        ("rel_b", "alice", 75),
    ):
        assert not plans.in_canary(release, user, bucket)
        assert plans.in_canary(release, user, bucket + 1)


def test_canary_versions_apply_to_users_in_the_rollout() -> None:
    cand = config_pb2.PipelineConfig(tenant="credit", version_id="cfv_canary")
    rt = config_pb2.RoutingConfig(version_id="cfv_rt2")
    canaries = [
        config_pb2.Canary(release_id="rel_a", tenant="credit", percent=40, pipeline=cand),
        config_pb2.Canary(release_id="rel_b", tenant="ops", percent=100, routing=rt),
        # policy 灰度不带候选包：只记提问人在不在里面
        config_pb2.Canary(release_id="rel_c", tenant="credit", percent=100, object="policy"),
    ]
    p = plans.Plans([pipeline()], ROUTING, [], canaries)
    # rel_a 下 bob 的桶号 28 < 40，alice 47 不在
    assert p.settings("credit", "bob").pipeline_version == "cfv_canary"
    assert p.settings("credit", "alice").pipeline_version == "cfv_p1"
    # 全局对象只对试点租户生效
    assert p.settings("ops", "alice").routing_version == "cfv_rt2"
    assert p.settings("credit", "alice").routing_version == "cfv_r1"
    assert p.settings("credit", "bob").canary_releases == ("rel_a", "rel_c")
    assert p.settings("credit", "alice").canary_releases == ("rel_c",)
    assert p.settings("ops", "alice").canary_releases == ("rel_b",)


def test_eval_candidate_applies_to_the_request() -> None:
    p = plans.Plans([pipeline()], ROUTING, [], [])
    # 门禁的候选不按用户分流，也不进 canary_releases（不是灰度）
    rt = config_pb2.Canary(
        object="routing", percent=100, routing=config_pb2.RoutingConfig(version_id="cfv_rt2")
    )
    got = p.settings("credit", "alice", rt)
    assert (got.routing_version, got.canary_releases) == ("cfv_rt2", ())
    # pipeline 的候选只对它的租户
    pl = config_pb2.Canary(
        object="pipeline",
        tenant="ops",
        percent=100,
        pipeline=config_pb2.PipelineConfig(tenant="ops", version_id="cfv_p2"),
    )
    assert p.settings("credit", "alice", pl).pipeline_version == "cfv_p1"
    assert p.settings("ops", "alice", pl).pipeline_version == "cfv_p2"
    # 用不到的对象（策略包在 retrieval）不带内容：照常用生效的
    assert p.settings("credit", "alice", config_pb2.Canary(object="policy")) == p.settings(
        "credit", "alice"
    )
