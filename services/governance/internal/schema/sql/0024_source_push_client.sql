-- 推送入库的客户端（入库管道详设 14.5）：业务系统经推送入库 API 写这个数据源时用的服务账号客户端 ID，注册时登记、随数据源审批；
-- 为空表示不接受推送。
ALTER TABLE governance.sources ADD COLUMN push_client text NOT NULL DEFAULT '';
