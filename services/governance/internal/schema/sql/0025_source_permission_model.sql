-- 数据源的权限模型（总体设计 ADR-14，部署配置清单 N8）：document（文档级，默认）或 partition（分区级）。注册时声明、随数据源审批，
-- 改为分区级需数据属主确认；connectors 按部署配置的分区授权文件同步，这里是审批记录。
ALTER TABLE governance.sources ADD COLUMN permission_model text NOT NULL DEFAULT 'document'
    CHECK (permission_model IN ('document', 'partition'));
