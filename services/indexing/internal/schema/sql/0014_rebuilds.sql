-- 全量重建（Z9，入库管道详设第 10 节、14.4）：申请走 idx.operations 双人复核，开始后记在 idx.rebuilds。
ALTER TABLE idx.operations DROP CONSTRAINT operations_action_check;
ALTER TABLE idx.operations ADD CONSTRAINT operations_action_check
    CHECK (action IN ('activate_index_version', 'revoke_tombstones', 'start_rebuild'));
ALTER TABLE idx.operations ADD COLUMN rebuild_id text NOT NULL DEFAULT '';
-- 同时只有一个待复核的重建申请
CREATE UNIQUE INDEX operations_one_pending_rebuild ON idx.operations (action)
    WHERE status = 'pending' AND action = 'start_rebuild';

-- 取消的重建把它的待发布版本标为 discarded：不会上线，也不能被切换过去
ALTER TABLE idx.index_versions DROP CONSTRAINT index_versions_status_check;
ALTER TABLE idx.index_versions ADD CONSTRAINT index_versions_status_check
    CHECK (status IN ('pending', 'live', 'retired', 'discarded'));

CREATE TABLE idx.rebuilds (
    rebuild_id      text        PRIMARY KEY,
    version         text        NOT NULL REFERENCES idx.index_versions (version),
    embedding_model text        NOT NULL,
    chunker_version text        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('running', 'completed', 'cancelled')),
    reason          text        NOT NULL,
    requested_by    text        NOT NULL,
    approved_by     text        NOT NULL,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    cancelled_by    text        NOT NULL DEFAULT ''
);
-- 同时只有一个进行中的重建
CREATE UNIQUE INDEX rebuilds_one_running ON idx.rebuilds ((true)) WHERE status = 'running';
