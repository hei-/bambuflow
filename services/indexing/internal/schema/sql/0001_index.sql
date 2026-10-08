-- P0 参考后端（ADR-9）：PostgreSQL + pgvector。只有 indexing 写这些表；retrieval 只读。
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS idx;

-- 索引版本：写入只发生在待发布版本（同一时刻至多一个），线上版本只通过别名切换改变（入库管道详设第 10 节）
CREATE TABLE idx.index_versions (
    version         text PRIMARY KEY,
    status          text        NOT NULL CHECK (status IN ('pending', 'live', 'retired')),
    -- 一个版本只允许一个嵌入模型版本，查询向量必须用同一模型生成
    embedding_model text        NOT NULL,
    chunker_version text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz
);
CREATE UNIQUE INDEX index_versions_one_pending ON idx.index_versions ((true)) WHERE status = 'pending';

CREATE TABLE idx.aliases (
    alias       text PRIMARY KEY,
    version     text        NOT NULL REFERENCES idx.index_versions (version),
    switched_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE idx.documents (
    doc_id       text PRIMARY KEY,
    source_id    text        NOT NULL,
    doc_version  bigint      NOT NULL,
    content_hash text        NOT NULL,
    title        text        NOT NULL,
    source_uri   text        NOT NULL,
    source_type  smallint    NOT NULL,
    owner        text        NOT NULL DEFAULT '',
    occurred_at  timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- 权限、密级与删除标记在文档级、不分版本，永远取最新值：回滚只恢复旧正文，不恢复旧权限（入库管道详设第 3 节）
CREATE SEQUENCE idx.acl_seq;
CREATE TABLE idx.doc_acl (
    doc_id         text PRIMARY KEY REFERENCES idx.documents (doc_id),
    source_id      text        NOT NULL,
    acl_tokens     text[]      NOT NULL,
    classification smallint    NOT NULL CHECK (classification BETWEEN 1 AND 4),
    state          text        NOT NULL CHECK (state IN ('active', 'tombstoned')),
    acl_seq        bigint      NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX doc_acl_tokens ON idx.doc_acl USING gin (acl_tokens);

CREATE TABLE idx.chunks (
    chunk_id        text     NOT NULL,
    doc_version     bigint   NOT NULL,
    doc_id          text     NOT NULL REFERENCES idx.documents (doc_id),
    parent_chunk_id text     NOT NULL,
    heading_path    text[]   NOT NULL,
    text            text     NOT NULL,
    text_hash       text     NOT NULL,
    span            jsonb    NOT NULL,
    embedding       vector(1024) NOT NULL,
    embedding_model text     NOT NULL,
    chunker_version text     NOT NULL,
    PRIMARY KEY (chunk_id, doc_version)
);
CREATE INDEX chunks_doc ON idx.chunks (doc_id, doc_version);

-- 版本清单：每个索引版本包含每份文档的哪个 doc_version
CREATE TABLE idx.version_docs (
    version     text   NOT NULL REFERENCES idx.index_versions (version),
    doc_id      text   NOT NULL REFERENCES idx.documents (doc_id),
    doc_version bigint NOT NULL,
    PRIMARY KEY (version, doc_id)
);
