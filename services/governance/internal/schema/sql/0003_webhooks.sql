-- 事件订阅（技术设计文档 13.7）：接入方登记的回调地址与要订阅的事件，以及每个事件对每个订阅的投递记录。
CREATE TABLE governance.webhook_subscriptions (
    subscription_id text        PRIMARY KEY,
    tenant          text        NOT NULL,
    url             text        NOT NULL,
    event_types     text[]      NOT NULL CHECK (cardinality(event_types) > 0),
    description     text        NOT NULL DEFAULT '',
    -- 签名密钥在 Vault 里的路径（KV v2，键 current 与轮换期间的 previous）；库里不存密钥
    secret_path     text        NOT NULL,
    active          boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX webhook_subscriptions_tenant ON governance.webhook_subscriptions (tenant) WHERE active;

-- 一个事件对一个订阅一行：同一事件被重复消费时不会重复投递（主键去重）
CREATE TABLE governance.webhook_deliveries (
    event_id         text        NOT NULL,
    subscription_id  text        NOT NULL REFERENCES governance.webhook_subscriptions,
    event_type       text        NOT NULL,
    -- 发给接收方的请求体（只有事件 ID、类型、时间与对象 ID）
    payload          bytea       NOT NULL,
    status           text        NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts         integer     NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    lease_until      timestamptz,
    last_status_code integer     NOT NULL DEFAULT 0,
    last_error       text        NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    -- 重试窗口（24 小时）的起点：首次入队或手工重放的时间
    window_start     timestamptz NOT NULL DEFAULT now(),
    delivered_at     timestamptz,
    PRIMARY KEY (event_id, subscription_id)
);

CREATE INDEX webhook_deliveries_due ON governance.webhook_deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX webhook_deliveries_subscription ON governance.webhook_deliveries (subscription_id, created_at DESC);
