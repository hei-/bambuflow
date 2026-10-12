-- 紧急拒绝规则的备用通道（governance 详设 8.2）：规则签名后还要发到事件流的独立主题，retrieval 直接订阅。
-- published_at 为空表示还没发出去，governance 每 10 秒重试。
ALTER TABLE governance.emergency_denials ADD COLUMN published_at timestamptz;
CREATE INDEX emergency_denials_unpublished ON governance.emergency_denials (created_at) WHERE published_at IS NULL;
