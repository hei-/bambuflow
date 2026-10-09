-- 撤销删除标记（Z6，入库管道详设第 5 节、14.4）：同样走双人复核，复核记录放在 idx.operations。
-- 撤销不针对索引版本，target_version 改为可空；条件（时间段、doc_ids）存在 params。
ALTER TABLE idx.operations ALTER COLUMN target_version DROP NOT NULL;
ALTER TABLE idx.operations ALTER COLUMN live_version SET DEFAULT '';
ALTER TABLE idx.operations DROP CONSTRAINT operations_action_check;
ALTER TABLE idx.operations ADD CONSTRAINT operations_action_check
    CHECK (action IN ('activate_index_version', 'revoke_tombstones'));
ALTER TABLE idx.operations
    ADD COLUMN source_id        text,
    ADD COLUMN params           jsonb,
    ADD COLUMN matched_docs     integer NOT NULL DEFAULT 0,
    ADD COLUMN revoked_docs     integer NOT NULL DEFAULT 0,
    ADD COLUMN refetch_event_id text    NOT NULL DEFAULT '';
-- 同一数据源同时只有一个待复核的撤销
CREATE UNIQUE INDEX operations_one_pending_revocation ON idx.operations (source_id)
    WHERE status = 'pending' AND action = 'revoke_tombstones';

-- 按数据源与删除时间选删除标记
CREATE INDEX doc_acl_tombstoned ON idx.doc_acl (source_id, tombstoned_at) WHERE state = 'tombstoned';

-- RefetchRequested 的发件表：与撤销在同一事务写入，提交后转发到事件流 ingest:refetch-requested（connectors 消费）。
-- 与 idx.outbox 分开：那张表的事件发到平台事件流，会投递给 Webhook 订阅方，这类内部事件不该出去
CREATE TABLE idx.refetch_outbox (
    event_id   text        PRIMARY KEY,
    payload    bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);
CREATE INDEX refetch_outbox_pending ON idx.refetch_outbox (created_at) WHERE sent_at IS NULL;
