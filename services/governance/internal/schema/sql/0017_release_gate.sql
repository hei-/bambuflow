-- 配置发布自动触发评测门禁（governance 详设 4.1 第 ③④ 步）：影响回答的对象与策略包发布后先是 gating，governance 用候选版本
-- 发起门禁（evaluation 的 EvalService），通过后转为 pending_approval，不通过、无法判定或超时为 gate_failed。
-- gate_started_at 是门禁发起的时间，按它判断超时（发起之前 evaluation 不可用的时间不算）。
ALTER TABLE governance.releases DROP CONSTRAINT releases_status_check;
ALTER TABLE governance.releases
    ADD CONSTRAINT releases_status_check CHECK (status IN ('gating', 'gate_failed', 'pending_approval', 'pending_second_approval',
                                                          'rolling_out', 'active', 'superseded', 'rolled_back', 'rejected', 'stale',
                                                          'expired')),
    ADD COLUMN gate_started_at timestamptz;
-- 作者不能批自己的发布（0009）：还没有人审批过的状态里原来漏了 expired（7 天未审批关闭的发布 approved_by 为空，
-- 关闭时会违反约束），这里一并补上，加上门禁的两个状态
ALTER TABLE governance.releases DROP CONSTRAINT releases_check;
ALTER TABLE governance.releases
    ADD CONSTRAINT releases_approver CHECK (status IN ('gating', 'gate_failed', 'pending_approval', 'rejected', 'stale', 'expired')
        OR source IN ('bootstrap', 'rollback') OR (approved_by <> '' AND approved_by <> requested_by));
