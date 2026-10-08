-- 异步批量任务（技术设计文档 13.5）：批次、条目与平台事件发件表，都在 qa/batch 自己的 schema 里。
CREATE SCHEMA IF NOT EXISTS batch;

CREATE TABLE batch.batches (
    batch_id        text        PRIMARY KEY,
    tenant          text        NOT NULL,
    -- 提交人（Principal.sub）与发起提交的客户端：查询、取结果只对提交人本人开放
    submitter       text        NOT NULL,
    actor           text        NOT NULL DEFAULT '',
    -- bambuflow.batch.v1.BatchMode 的编号
    mode            integer     NOT NULL,
    -- gateway 签发的委托，每条执行前凭它换发 Principal；批次结束即清空，不再留着可用的凭证
    delegation      text        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    idempotency_key text,
    request_digest  text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    completed_at    timestamptz,
    -- 结果保留截止时间：结束后 7 天，过期由清理任务连同条目一起删除
    expires_at      timestamptz
);

CREATE UNIQUE INDEX batches_idempotency ON batch.batches (tenant, submitter, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX batches_expires_at ON batch.batches (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE batch.items (
    batch_id        text        NOT NULL REFERENCES batch.batches ON DELETE CASCADE,
    -- 提交顺序，结果按它输出
    seq             integer     NOT NULL,
    custom_id       text        NOT NULL,
    question        text        NOT NULL,
    -- bambuflow.common.v1.Filters 的序列化；没有过滤条件时为空
    filters         bytea,
    status          text        NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    -- 执行中的条目在此之前归领取它的实例；实例中途退出时，过期后由别的实例重新领取
    lease_until     timestamptz,
    answer_id       text,
    -- bambuflow.answer.v1.AnswerResponse 的序列化
    result          bytea,
    error_type      text,
    error_title     text,
    finished_at     timestamptz,
    PRIMARY KEY (batch_id, seq),
    UNIQUE (batch_id, custom_id)
);

CREATE INDEX items_due ON batch.items (next_attempt_at) WHERE status = 'queued';
CREATE INDEX items_lease ON batch.items (lease_until) WHERE status = 'running';

-- 平台事件发件表（libs/platform-go/outbox）：batch.completed / batch.failed 与批次结束在同一事务写入
CREATE TABLE batch.outbox (
    event_id   text        PRIMARY KEY,
    payload    bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);

CREATE INDEX outbox_pending ON batch.outbox (created_at) WHERE sent_at IS NULL;
