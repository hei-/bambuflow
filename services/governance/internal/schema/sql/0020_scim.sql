-- SCIM 推送的资源（gateway 详设 3.5）：上游按 SCIM 2.0 推送用户与群组，这里存最近一次的表示，
-- 供上游查询（GET）与 PATCH 在其上修改；平台身份库（identities、identity_groups）由它推导，在同一个事务里写入。
-- id 就是平台账号（用户）或群组名（群组）：租户配置与策略按群组名引用，改名等于换了一个群组，所以不支持改名。
CREATE TABLE identity.scim_resources (
    source     text        NOT NULL,
    type       text        NOT NULL CHECK (type IN ('User', 'Group')),
    id         text        NOT NULL,
    body       jsonb       NOT NULL,
    version    bigint      NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, type, id)
);
-- 上游按 externalId 或不区分大小写的 userName / displayName 查找已有资源
CREATE INDEX scim_resources_external_id ON identity.scim_resources (source, type, (body->>'externalId'));
CREATE INDEX scim_resources_lower_id ON identity.scim_resources (source, type, lower(id));
