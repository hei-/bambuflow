-- 插件登记（Z10，governance 详设 3.1、3.4，技术设计文档 12.4）：以 governance 的数据库为唯一事实来源。
-- 评估结果由 CI 经 PluginEvaluationService 写入；Tool 插件另需信息安全评审。
CREATE TABLE governance.plugins (
    plugin_id       text        PRIMARY KEY,
    kind            text        NOT NULL CHECK (kind IN ('connector', 'parser', 'chunker', 'retriever', 'reranker', 'model', 'tool')),
    name            text        NOT NULL,
    version         text        NOT NULL,
    host            text        NOT NULL,
    description     text        NOT NULL,
    tool_scopes     text[]      NOT NULL DEFAULT '{}',
    registered_by   text        NOT NULL,
    registered_at   timestamptz NOT NULL DEFAULT now(),
    security_review text        NOT NULL CHECK (security_review IN ('not_required', 'pending', 'approved', 'rejected')),
    reviewed_by     text        NOT NULL DEFAULT '',
    reviewed_at     timestamptz,
    review_note     text        NOT NULL DEFAULT '',
    UNIQUE (name, version),
    CHECK ((kind = 'tool') = (security_review <> 'not_required'))
);
CREATE INDEX plugins_registered ON governance.plugins (registered_at DESC);

-- 每个租户只保留最近一次评估
CREATE TABLE governance.plugin_evaluations (
    plugin_id    text        NOT NULL REFERENCES governance.plugins (plugin_id),
    tenant       text        NOT NULL,
    passed       boolean     NOT NULL,
    report_ref   text        NOT NULL,
    evaluated_at timestamptz NOT NULL,
    PRIMARY KEY (plugin_id, tenant)
);
