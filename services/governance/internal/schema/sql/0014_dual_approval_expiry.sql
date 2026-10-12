-- 策略包的发布要两位不同的人审批（作者不算），至少一位是信息安全或数据属主（governance 详设 4.2、9.1）：
-- 第一位审批后状态为 pending_second_approval，第二位审批后生效。security_approved 记两位里是否已有信息安全或数据属主。
-- 7 天未审批的发布与变更单自动关闭，状态记为 expired（详设 4.1）。
ALTER TABLE governance.releases DROP CONSTRAINT releases_status_check;
ALTER TABLE governance.releases
    ADD CONSTRAINT releases_status_check CHECK (status IN ('pending_approval', 'pending_second_approval', 'active', 'superseded',
                                                          'rolled_back', 'rejected', 'stale', 'expired')),
    ADD COLUMN second_approved_by text NOT NULL DEFAULT '',
    ADD COLUMN second_approved_at timestamptz,
    ADD COLUMN security_approved  boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT releases_second_approver CHECK (second_approved_by = ''
        OR (second_approved_by <> requested_by AND second_approved_by <> approved_by));
ALTER TABLE governance.change_requests DROP CONSTRAINT change_requests_status_check;
ALTER TABLE governance.change_requests
    ADD CONSTRAINT change_requests_status_check CHECK (status IN ('open', 'approved', 'rejected', 'stale', 'merge_request', 'expired'));
