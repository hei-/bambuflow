-- 受控词表（governance 详设 6.1）：平台统一的文档类型、发文部门、主题三套。
-- 编码一旦使用就不删除、不改义，只能停用并指定替代编码；所以只有插入与更新，没有删除。
CREATE SCHEMA IF NOT EXISTS governance;

CREATE TABLE governance.vocabulary_terms (
    vocabulary  text        NOT NULL CHECK (vocabulary IN ('doc_type', 'dept', 'topic')),
    code        text        NOT NULL,
    name        text        NOT NULL,
    synonyms    text[]      NOT NULL DEFAULT '{}',
    parent      text        NOT NULL DEFAULT '',
    status      text        NOT NULL CHECK (status IN ('active', 'deprecated')),
    replaced_by text        NOT NULL DEFAULT '',
    -- 维护部门（审批人）
    owner       text        NOT NULL DEFAULT '',
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (vocabulary, code),
    CHECK (status = 'active' OR replaced_by <> '')
);
