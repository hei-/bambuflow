-- 平台事件发件表（libs/platform-go/outbox）：index.version.live 与别名切换在同一事务写入，提交后由 indexing serve 转发到事件流。
CREATE TABLE idx.outbox (
    event_id   text        PRIMARY KEY,
    payload    bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);

CREATE INDEX outbox_pending ON idx.outbox (created_at) WHERE sent_at IS NULL;
