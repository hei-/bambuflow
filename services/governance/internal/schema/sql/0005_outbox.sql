-- 平台事件发件表（libs/platform-go/outbox）：source.revoked 与撤销数据源同一事务写入，由 governance serve 转发到事件流。
CREATE TABLE governance.outbox (
    event_id   text        PRIMARY KEY,
    payload    bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);

CREATE INDEX outbox_pending ON governance.outbox (created_at) WHERE sent_at IS NULL;
