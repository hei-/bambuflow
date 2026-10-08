-- 回答记录与反馈（技术设计文档 13.1、9.2、F8）：回放与反馈的依据，在 orchestrator 自己的 schema 里。
CREATE SCHEMA IF NOT EXISTS orchestrator;

CREATE TABLE orchestrator.answer_records (
    answer_id      text        PRIMARY KEY,
    tenant         text        NOT NULL,
    -- 提问人（Principal.sub）与代用户调用的客户端：回放只对提问人本人与同一租户的审计岗开放，反馈只对提问人开放
    asker          text        NOT NULL,
    actor          text        NOT NULL DEFAULT '',
    -- 入口的调用目的：answer、mcp、batch
    purpose        text        NOT NULL,
    -- 已做 PII 掩码；涉及受限级数据时为空（body_withheld）
    question       text        NOT NULL,
    body_withheld  boolean     NOT NULL,
    -- 原回答的句子数，反馈指向句子时据此校验下标
    sentence_count integer     NOT NULL,
    -- bambuflow.answer.v1.AnswerResponse 的序列化，不含 audit
    answer         bytea       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- 保留期待合规确认（部署配置清单 N4），确定后按时间清理
CREATE INDEX answer_records_created_at ON orchestrator.answer_records (created_at);

CREATE TABLE orchestrator.answer_feedback (
    feedback_id     text        PRIMARY KEY,
    answer_id       text        NOT NULL REFERENCES orchestrator.answer_records ON DELETE CASCADE,
    tenant          text        NOT NULL,
    submitter       text        NOT NULL,
    rating          text        NOT NULL CHECK (rating IN ('useful', 'wrong', 'bad_citation')),
    sentence_index  integer     CHECK (sentence_index >= 0),
    -- 已做 PII 掩码，不超过 500 字
    comment         text        NOT NULL DEFAULT '',
    idempotency_key text        NOT NULL,
    -- 请求内容的摘要：同一幂等键对应不同内容时报冲突
    request_digest  text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant, submitter, idempotency_key)
);

-- 反馈聚合（evaluation 详设第 1 节在线评估）按租户与时间读取
CREATE INDEX answer_feedback_tenant_created ON orchestrator.answer_feedback (tenant, created_at);
CREATE INDEX answer_feedback_answer ON orchestrator.answer_feedback (answer_id);
