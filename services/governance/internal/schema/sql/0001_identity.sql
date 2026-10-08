-- 平台身份库（技术设计文档 8.1、gateway 详设 3.5）：只由身份同步写入；gateway、retrieval 只读。
CREATE SCHEMA IF NOT EXISTS identity;

CREATE TABLE identity.identities (
    username     text PRIMARY KEY,               -- 公司目录中的账号，即 Principal.sub
    display_name text        NOT NULL DEFAULT '',
    dept         text        NOT NULL DEFAULT '',
    title        text        NOT NULL DEFAULT '',
    -- bambuflow.common.v1.Classification 的编号；映射表缺失的等级按最低（公开）处理
    clearance    smallint    NOT NULL CHECK (clearance BETWEEN 1 AND 4),
    region       text        NOT NULL DEFAULT '',
    status       text        NOT NULL CHECK (status IN ('active', 'disabled')),
    source       text        NOT NULL,
    source_dn    text        NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- 不对 identities 设外键：群组里可能有不在同步范围内的成员，删除用户时也要保留历史可追溯
CREATE TABLE identity.identity_groups (
    grp      text NOT NULL,
    username text NOT NULL,
    source   text NOT NULL,
    PRIMARY KEY (grp, username)
);
CREATE INDEX identity_groups_username ON identity.identity_groups (username);

-- 每个同步源的状态；gateway 据 last_success_at 判断身份是否新鲜（超过 15 分钟 fail-closed）
CREATE TABLE identity.sync_state (
    source          text PRIMARY KEY,
    last_success_at timestamptz,
    last_full_at    timestamptz,
    users_cursor    text NOT NULL DEFAULT '',
    groups_cursor   text NOT NULL DEFAULT '',
    last_error      text NOT NULL DEFAULT '',
    last_attempt_at timestamptz
);
