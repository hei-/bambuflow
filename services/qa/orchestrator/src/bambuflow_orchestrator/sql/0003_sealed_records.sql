-- 问题原文与受限级回答快照的信封加密（技术设计文档 8.4，部署配置清单 P3）：每条回答一个数据密钥，
-- 由密钥服务（默认 Vault transit）的主密钥包裹后存 sealed_key。没有配置密钥服务时这几列为空，行为与之前相同。
ALTER TABLE orchestrator.answer_records
    -- 问题原文（未掩码），附加数据 answer_id:question
    ADD COLUMN IF NOT EXISTS sealed_question bytea,
    -- 受限级回答的完整回答（不含 audit），附加数据 answer_id:answer；非受限级为空
    ADD COLUMN IF NOT EXISTS sealed_answer   bytea,
    ADD COLUMN IF NOT EXISTS sealed_key      text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS key_id          text NOT NULL DEFAULT '';
