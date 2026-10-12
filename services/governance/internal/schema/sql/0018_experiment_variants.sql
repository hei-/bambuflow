-- A/B 实验组替换的配置版本（H25f，orchestrator 详设 12.3）：pipeline、routing、回答模板的 cfv_ 版本，立项时已核对门禁通过。
ALTER TABLE governance.experiments ADD COLUMN config_versions text[] NOT NULL DEFAULT '{}';
-- 只替换配置版本、不改阈值的实验也合法；两样都没有的实验两组完全相同（校验在 governance 代码里，这里兜底）
ALTER TABLE governance.experiments ADD CONSTRAINT experiments_has_override
    CHECK (support_threshold IS NOT NULL OR min_coverage IS NOT NULL OR cardinality(config_versions) > 0);
