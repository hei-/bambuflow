-- 按数据源、按租户的紧急拒绝（governance 的紧急拒绝规则经 IndexAdminService.AddDenyRules 写入，governance 详设 4.2）。
-- 只能新增：解除走变更单。retrieval 在过滤（必含条件）与返回前复核两处读取；按租户的在评估策略时读取。
CREATE TABLE idx.deny_scopes (
    kind       text        NOT NULL CHECK (kind IN ('source', 'tenant')),
    value      text        NOT NULL,
    reason     text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, value)
);
