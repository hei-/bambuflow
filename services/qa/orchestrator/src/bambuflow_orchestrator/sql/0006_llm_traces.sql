-- LLM 追踪库（技术设计文档 14.2，orchestrator 详设 13.3）：一次回答的全部模型调用（提示词与输出）与逐句校验结果，
-- 信封加密后整体存 sealed（附加数据 trace_id），数据密钥由密钥服务的主密钥包裹后存 sealed_key。
-- 上下文里有受限级内容的回答不写。docs 是这次回答用到的文档（数据源:文档，清单路径列出的只有文档），
-- 文档物理删除时按它删掉记录（部署配置清单 P2）；保留期与回答记录一致（purge）。
CREATE TABLE IF NOT EXISTS orchestrator.llm_traces (
    trace_id    text PRIMARY KEY,
    answer_id   text NOT NULL,
    tenant      text NOT NULL,
    docs        text[] NOT NULL DEFAULT '{}',
    sealed      bytea NOT NULL,
    sealed_key  text NOT NULL,
    key_id      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS llm_traces_docs ON orchestrator.llm_traces USING gin (docs);
CREATE INDEX IF NOT EXISTS llm_traces_created_at ON orchestrator.llm_traces (created_at);
