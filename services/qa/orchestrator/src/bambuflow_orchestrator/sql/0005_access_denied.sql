-- 返回前复核拦下的块数（灰度观察的越权拦截数，governance 详设 4.1 第 ⑨ 步）
ALTER TABLE orchestrator.answer_records ADD COLUMN IF NOT EXISTS access_denied integer NOT NULL DEFAULT 0;
