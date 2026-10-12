-- 灰度（governance 详设 3.2、4.1、8.3）：影响回答的对象与策略包审批后先进灰度，候选版本只对一部分用户生效——
-- 租户对象（pipeline、租户策略层）对该租户 10% 的用户，全局对象（routing、回答模板、全局与数据源策略层）对一个试点租户的
-- 全部用户。同一对象同一作用域同时只有一个灰度（主键）。全量后切生效指针并删掉这一行；终止灰度直接删掉。
CREATE TABLE governance.rollouts (
    object     text        NOT NULL,
    scope      text        NOT NULL,
    release_id text        NOT NULL REFERENCES governance.releases (release_id),
    version_id text        NOT NULL REFERENCES governance.config_versions (version_id),
    tenant     text        NOT NULL,
    percent    integer     NOT NULL CHECK (percent BETWEEN 1 AND 100),
    started_at timestamptz NOT NULL,
    PRIMARY KEY (object, scope)
);

ALTER TABLE governance.releases DROP CONSTRAINT releases_status_check;
ALTER TABLE governance.releases
    ADD CONSTRAINT releases_status_check CHECK (status IN ('pending_approval', 'pending_second_approval', 'rolling_out', 'active',
                                                          'superseded', 'rolled_back', 'rejected', 'stale', 'expired')),
    -- 全局对象灰度的试点租户；CI 发布时不指定则审批时按“第一个试点阶段的租户”选
    ADD COLUMN canary_tenant text NOT NULL DEFAULT '';
