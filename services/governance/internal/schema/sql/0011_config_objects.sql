-- 配置对象补齐（governance 详设 3.1）：除租户配置外，还有 pipeline（租户）、routing（global）与策略包的各层
-- （policy，scope 为 global、source:<数据源>、tenant:<租户>）。
-- analysis 是策略层发布前差异分析的结果：新版本下可见范围变大的代表性主体（详设 5.2 第 4 步），审批时展示。
ALTER TABLE governance.config_versions DROP CONSTRAINT config_versions_object_check;
ALTER TABLE governance.config_versions
    ADD CONSTRAINT config_versions_object_check CHECK (object IN ('tenant', 'pipeline', 'routing', 'policy')),
    ADD COLUMN analysis text[] NOT NULL DEFAULT '{}';
