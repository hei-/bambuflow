-- 授权申请（governance 详设 6.2、9.1）：租户负责人申请、数据源所属部门的数据属主审批，审批通过即写入 source_grants。
CREATE TABLE governance.grant_applications (
    application_id text        PRIMARY KEY,
    source_id      text        NOT NULL REFERENCES governance.sources,
    tenant         text        NOT NULL,
    reason         text        NOT NULL,
    -- 待审批超过 7 天的在读出与审批时按过期处理，同一数据源与租户再申请时才落库为 expired
    status         text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'withdrawn', 'expired')),
    applied_by     text        NOT NULL,
    applied_at     timestamptz NOT NULL DEFAULT now(),
    decided_by     text        NOT NULL DEFAULT '',
    decided_at     timestamptz,
    decision_note  text        NOT NULL DEFAULT '',
    CHECK (status IN ('pending', 'expired') OR (decided_by <> '' AND decided_at IS NOT NULL))
);
-- 同一数据源与租户同时只有一个待审批的申请
CREATE UNIQUE INDEX grant_applications_pending ON governance.grant_applications (source_id, tenant) WHERE status = 'pending';
CREATE INDEX grant_applications_source ON governance.grant_applications (source_id, applied_at DESC);
CREATE INDEX grant_applications_tenant ON governance.grant_applications (tenant, applied_at DESC);

-- 授权从哪个申请来（审批人、时间与依据在申请里）；数据属主直接授权的为空
ALTER TABLE governance.source_grants ADD COLUMN application_id text NOT NULL DEFAULT '';
