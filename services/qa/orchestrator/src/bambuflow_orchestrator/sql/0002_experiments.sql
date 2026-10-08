-- A/B 分析与在线抽样评审要的字段（evaluation 详设 7.1、7.2）：实验分组、耗时与 token 用量。
-- 用量单独成列、不放进回答契约：回放读的是 answer，不应再计一次配额。
ALTER TABLE orchestrator.answer_records
    ADD COLUMN experiment_id     text    NOT NULL DEFAULT '',
    -- control、treatment、excluded；没有实验时为空
    ADD COLUMN experiment_arm    text    NOT NULL DEFAULT '',
    ADD COLUMN latency_ms        integer NOT NULL DEFAULT 0,
    ADD COLUMN prompt_tokens     bigint  NOT NULL DEFAULT 0,
    ADD COLUMN completion_tokens bigint  NOT NULL DEFAULT 0;

-- 导出按租户与时间、按实验与时间读取
CREATE INDEX answer_records_tenant_created ON orchestrator.answer_records (tenant, created_at);
CREATE INDEX answer_records_experiment ON orchestrator.answer_records (experiment_id, created_at)
    WHERE experiment_id <> '';
