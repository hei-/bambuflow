-- 数据源登记、租户授权与紧急拒绝规则（governance 详设 3.4、6.2、4.2）：以 governance 的数据库为唯一事实来源，
-- 只由 governance 写。授权经配置快照下发给 retrieval；紧急拒绝规则经 IndexAdminService 写进索引库的拒绝名单。

-- 数据源（总体设计 4.1 sources；同步游标等连接器私有字段在 connectors，不在这里）
CREATE TABLE governance.sources (
    source_id              text        PRIMARY KEY,
    type                   text        NOT NULL CHECK (type IN ('wiki', 'mail', 'file', 'image', 'table', 'log', 'db')),
    -- 数据属主部门：审批、撤销、授权与按源的紧急拒绝只能由该部门的数据属主做
    owner_dept             text        NOT NULL,
    connector_plugin       text        NOT NULL DEFAULT '',
    -- 源系统默认密级，bambuflow.common.v1.Classification 的编号
    default_classification smallint    NOT NULL CHECK (default_classification BETWEEN 1 AND 4),
    description            text        NOT NULL DEFAULT '',
    -- pending：已注册、待属主审批；active：已审批；revoked：已撤销（不再恢复）
    status                 text        NOT NULL CHECK (status IN ('pending', 'active', 'revoked')),
    created_by             text        NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    -- 审批记录（governance 详设 6.2）：审批人、时间、依据（如内部工单号）
    approved_by            text        NOT NULL DEFAULT '',
    approved_at            timestamptz,
    approval_basis         text        NOT NULL DEFAULT '',
    revoked_by             text        NOT NULL DEFAULT '',
    revoked_at             timestamptz,
    revoke_reason          text        NOT NULL DEFAULT '',
    CHECK (status = 'pending' OR (approved_by <> '' AND approved_at IS NOT NULL)),
    CHECK (status <> 'revoked' OR (revoked_by <> '' AND revoked_at IS NOT NULL))
);
CREATE INDEX sources_owner ON governance.sources (owner_dept);

-- 租户授权：取消授权即删除这一行（收紧类，governance 详设 4.2）
CREATE TABLE governance.source_grants (
    source_id  text        NOT NULL REFERENCES governance.sources,
    tenant     text        NOT NULL,
    granted_by text        NOT NULL,
    granted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, tenant)
);

-- 紧急拒绝规则：只能新增，没有删除（解除走变更单）；同一对象只登记一条
CREATE TABLE governance.emergency_denials (
    denial_id  text        PRIMARY KEY,
    scope      text        NOT NULL CHECK (scope IN ('source', 'document', 'tenant')),
    target     text        NOT NULL,
    reason     text        NOT NULL,
    created_by text        NOT NULL,
    -- 入口的 trace_id，与 gateway 的管理审计关联
    trace_id   text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    -- 写进索引库拒绝名单的时间；为空表示尚未生效，governance 持续重试
    applied_at timestamptz,
    UNIQUE (scope, target)
);
CREATE INDEX emergency_denials_pending ON governance.emergency_denials (created_at) WHERE applied_at IS NULL;
