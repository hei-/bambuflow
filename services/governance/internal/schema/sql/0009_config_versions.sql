-- 配置对象的版本、生效指针、变更单与发布（Z13、Z14，governance 详设 3.2、3.4、4、8.3）。
-- 现在的对象只有租户配置（object = tenant，scope = 租户名）。版本不可变；生效指针只由发布改。
CREATE TABLE governance.config_versions (
    version_id   text        PRIMARY KEY,
    object       text        NOT NULL CHECK (object IN ('tenant')),
    scope        text        NOT NULL,
    number       integer     NOT NULL CHECK (number > 0),
    content      text        NOT NULL,
    content_hash text        NOT NULL,
    created_by   text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    source_ref   text        NOT NULL,
    UNIQUE (object, scope, number)
);

CREATE TABLE governance.active_pointers (
    object     text        NOT NULL,
    scope      text        NOT NULL,
    version_id text        NOT NULL REFERENCES governance.config_versions (version_id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object, scope)
);

CREATE TABLE governance.releases (
    release_id      text        PRIMARY KEY,
    object          text        NOT NULL,
    scope           text        NOT NULL,
    version_id      text        NOT NULL REFERENCES governance.config_versions (version_id),
    -- 发布时的生效版本，回退切回它；新建对象时为空
    from_version_id text        REFERENCES governance.config_versions (version_id),
    source          text        NOT NULL CHECK (source IN ('ci', 'change_request', 'rollback', 'bootstrap')),
    source_ref      text        NOT NULL,
    gate_id         text        NOT NULL DEFAULT '',
    status          text        NOT NULL CHECK (status IN ('pending_approval', 'active', 'superseded', 'rolled_back', 'rejected', 'stale')),
    reason          text        NOT NULL,
    requested_by    text        NOT NULL,
    requested_at    timestamptz NOT NULL,
    approved_by     text        NOT NULL DEFAULT '',
    approved_at     timestamptz,
    rolled_back_by  text        NOT NULL DEFAULT '',
    rolled_back_at  timestamptz,
    -- 作者不能批自己的发布（governance 详设 4.1）；初始导入与回退由执行人一人完成
    CHECK (status = 'pending_approval' OR status = 'rejected' OR status = 'stale' OR source IN ('bootstrap', 'rollback')
           OR (approved_by <> '' AND approved_by <> requested_by))
);
CREATE INDEX releases_scope ON governance.releases (object, scope, requested_at DESC);

CREATE TABLE governance.change_requests (
    chg_id          text        PRIMARY KEY,
    object          text        NOT NULL,
    scope           text        NOT NULL,
    base_version_id text        REFERENCES governance.config_versions (version_id),
    version_id      text        NOT NULL REFERENCES governance.config_versions (version_id),
    reason          text        NOT NULL,
    gate_id         text        NOT NULL DEFAULT '',
    author          text        NOT NULL,
    created_at      timestamptz NOT NULL,
    status          text        NOT NULL CHECK (status IN ('open', 'approved', 'rejected', 'stale')),
    decided_by      text        NOT NULL DEFAULT '',
    decided_at      timestamptz,
    reject_reason   text        NOT NULL DEFAULT '',
    release_id      text        NOT NULL DEFAULT '',
    -- 作者不能批自己的变更单（可以撤回，记为 rejected）
    CHECK (status <> 'approved' OR decided_by <> author)
);
-- 同一对象同时只有一个待审批的变更单
CREATE UNIQUE INDEX change_requests_one_open ON governance.change_requests (object, scope) WHERE status = 'open';
