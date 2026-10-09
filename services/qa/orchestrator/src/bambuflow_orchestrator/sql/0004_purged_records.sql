-- 回答记录的保留期（技术设计文档 9.2：审计字段保留，原文到期销毁密钥）：到期后销毁数据密钥与密文、清掉掩码问题与正文，
-- 只留标识、版本与状态；purged_at 记下销毁时间。
ALTER TABLE orchestrator.answer_records ADD COLUMN IF NOT EXISTS purged_at timestamptz;
CREATE INDEX IF NOT EXISTS answer_records_unpurged ON orchestrator.answer_records (created_at) WHERE purged_at IS NULL;
