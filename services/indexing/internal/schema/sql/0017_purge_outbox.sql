-- 物理删除后的 DocumentPurged（K6）：与删除凭证同一事务写入，由 indexing 转发到事件流 ingest:document-purged，
-- connectors 据此删掉对象存储里的原文与文档包，再把完成时间补记进删除凭证
CREATE TABLE idx.purge_outbox (
    event_id   text        PRIMARY KEY,
    payload    bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);
CREATE INDEX purge_outbox_unsent ON idx.purge_outbox (created_at) WHERE sent_at IS NULL;
