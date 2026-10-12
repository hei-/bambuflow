-- 审计索引（技术设计文档 4.1 audit_index、8.3）：审计原文在只追加的审计桶，这里只存检索字段与原文位置，只追加。
-- governance 的索引任务（internal/auditquery）把审计桶里的三类记录写进来：gateway 的入口审计（access）、各服务的变更审计（change）、
-- 评测回放（eval，evaluation 详设 4.2）。按月范围分区，分区由索引任务在写入该月第一条记录前创建。
CREATE TABLE governance.audit_index (
    ts              timestamptz NOT NULL,
    -- 记录在审计桶里的标识：同一条记录重复索引（段重送、任务重跑）落到同一行
    record_id       text        NOT NULL,
    kind            text        NOT NULL CHECK (kind IN ('access', 'change', 'eval')),
    -- 写这条记录的服务
    source          text        NOT NULL,
    -- 原文位置：对象键与行号（从 1 起）
    object_key      text        NOT NULL,
    line            integer     NOT NULL CHECK (line > 0),
    trace_id        text        NOT NULL DEFAULT '',
    tenant          text        NOT NULL DEFAULT '',
    -- 入口审计与评测回放是提问人，变更审计是操作人
    user_id         text        NOT NULL DEFAULT '',
    -- 入口审计是接口名，变更审计是动作
    action          text        NOT NULL DEFAULT '',
    -- 变更审计的对象 ID（文档、数据源、发布等）
    object_id       text        NOT NULL DEFAULT '',
    -- 由块 ID 得出的文档（技术设计文档 8.3：展示与上下文进索引，候选只在原文里）与枚举类回答列出的文档
    shown_doc_ids   text[]      NOT NULL DEFAULT '{}',
    context_doc_ids text[]      NOT NULL DEFAULT '{}',
    listed_doc_ids  text[]      NOT NULL DEFAULT '{}',
    PRIMARY KEY (ts, record_id)
) PARTITION BY RANGE (ts);

CREATE INDEX audit_index_user ON governance.audit_index (user_id, ts);
CREATE INDEX audit_index_object ON governance.audit_index (object_id, ts) WHERE object_id <> '';
-- “这份文档展示给过哪些回答和用户”（技术设计文档 4.1）
CREATE INDEX audit_index_shown ON governance.audit_index USING gin (shown_doc_ids);
CREATE INDEX audit_index_context ON governance.audit_index USING gin (context_doc_ids);
CREATE INDEX audit_index_listed ON governance.audit_index USING gin (listed_doc_ids);

-- 已经索引过的审计对象：索引任务据此跳过
CREATE TABLE governance.audit_index_objects (
    object_key text        PRIMARY KEY,
    records    integer     NOT NULL,
    indexed_at timestamptz NOT NULL DEFAULT now()
);

-- 索引任务的状态（如首次全量补录是否完成）
CREATE TABLE governance.audit_index_state (
    name  text PRIMARY KEY,
    value text NOT NULL
);
