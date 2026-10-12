-- 回答模板作为配置对象（object = prompt，scope 为 answer 或 agent-answer，技术设计文档 7.7、governance 详设 3.1）。
ALTER TABLE governance.config_versions DROP CONSTRAINT config_versions_object_check;
ALTER TABLE governance.config_versions
    ADD CONSTRAINT config_versions_object_check CHECK (object IN ('tenant', 'pipeline', 'routing', 'policy', 'prompt'));
