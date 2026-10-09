-- 各服务实例上报的快照加载结果（governance 详设 7.1 Ack、8.2）：每个实例一行，记最近一次上报。
-- 用于灰度跟踪与紧急规则的生效确认；加载失败的实例在用旧快照。
CREATE TABLE governance.config_acks (
    service          text        NOT NULL,
    instance_id      text        NOT NULL,
    snapshot_version text        NOT NULL,
    loaded           boolean     NOT NULL,
    reason           text        NOT NULL DEFAULT '',
    acked_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service, instance_id)
);
