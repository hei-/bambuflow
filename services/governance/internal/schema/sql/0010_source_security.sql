-- 受限级数据源的信息安全审批（governance 详设 6.2、9.1）：受限级注册后要本部门属主与信息安全各批一次才生效，
-- 两项审批不能是同一人。
ALTER TABLE governance.sources
    ADD COLUMN security_approved_by    text        NOT NULL DEFAULT '',
    ADD COLUMN security_approved_at    timestamptz,
    ADD COLUMN security_approval_basis text        NOT NULL DEFAULT '',
    ADD CONSTRAINT sources_restricted_security CHECK (
        status = 'pending' OR default_classification < 4 OR (security_approved_by <> '' AND security_approved_at IS NOT NULL)),
    ADD CONSTRAINT sources_two_approvers CHECK (security_approved_by = '' OR security_approved_by <> approved_by);
CREATE INDEX sources_created ON governance.sources (created_at DESC);
