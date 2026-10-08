-- 需要双人复核的操作（入库管道详设 14.4）：第一次调用只创建待复核的操作（op_），另一位平台运维复核后才执行。
-- 现在只有索引版本的切换（回滚或切回）；谁申请、谁复核、为什么没执行都留在这里。
CREATE TABLE idx.operations (
    operation_id   text        PRIMARY KEY,
    action         text        NOT NULL CHECK (action IN ('activate_index_version')),
    target_version text        NOT NULL REFERENCES idx.index_versions (version),
    -- 申请时的线上版本
    live_version   text        NOT NULL,
    reason         text        NOT NULL,
    requested_by   text        NOT NULL,
    requested_at   timestamptz NOT NULL,
    status         text        NOT NULL CHECK (status IN ('pending', 'executed', 'failed', 'expired')),
    approved_by    text        NOT NULL DEFAULT '',
    approved_at    timestamptz,
    failure_reason text        NOT NULL DEFAULT '',
    expires_at     timestamptz NOT NULL,
    CHECK (status <> 'executed' OR (approved_by <> '' AND approved_by <> requested_by))
);
-- 同一版本同时只有一个待复核的切换
CREATE UNIQUE INDEX operations_one_pending ON idx.operations (action, target_version) WHERE status = 'pending';
