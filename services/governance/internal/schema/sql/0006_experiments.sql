-- A/B 实验登记（governance 详设 3.1、3.4，技术设计文档 10.5）：以 governance 的数据库为唯一事实来源，只由 governance 写；
-- 运行中的实验经配置快照下发给 orchestrator，由它按 user_id 与 exp_id 分组（orchestrator 详设 12.3）。
CREATE TABLE governance.experiments (
    exp_id                text        PRIMARY KEY,
    tenant                text        NOT NULL,
    hypothesis            text        NOT NULL,
    -- useful_rate、insufficient_evidence_rate（契约 PrimaryMetric）
    primary_metric        text        NOT NULL CHECK (primary_metric IN ('useful_rate', 'insufficient_evidence_rate')),
    baseline_rate         double precision NOT NULL CHECK (baseline_rate > 0 AND baseline_rate < 1),
    min_detectable_effect double precision NOT NULL CHECK (min_detectable_effect > 0 AND min_detectable_effect < 1),
    sample_size_per_arm   bigint      NOT NULL CHECK (sample_size_per_arm > 0),
    traffic_percent       integer     NOT NULL CHECK (traffic_percent IN (5, 20, 50)),
    -- 实验组替换的对象（契约 ExperimentOverrides）
    support_threshold     double precision CHECK (support_threshold BETWEEN 0.3 AND 0.9),
    min_coverage          double precision CHECK (min_coverage BETWEEN 0.5 AND 1),
    status                text        NOT NULL CHECK (status IN ('running', 'stopped')),
    created_by            text        NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    stopped_by            text        NOT NULL DEFAULT '',
    stopped_at            timestamptz,
    stop_reason           text        NOT NULL DEFAULT '',
    CHECK (status = 'running' OR (stopped_by <> '' AND stopped_at IS NOT NULL))
);

-- 同一租户同时只有一个运行中的实验：分组不必处理多个实验叠加
CREATE UNIQUE INDEX experiments_one_running ON governance.experiments (tenant) WHERE status = 'running';
CREATE INDEX experiments_created ON governance.experiments (created_at DESC);
