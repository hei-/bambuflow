-- 权限快速通道与紧急拒绝名单（P2-T1，入库管道详设第 9 节、K9）。
-- acl_occurred_at：最近一次权限变化在源端发生的时间，乱序到达的旧权限事件按它丢弃。
ALTER TABLE idx.doc_acl ADD COLUMN acl_occurred_at timestamptz;

-- 紧急拒绝名单：名单里的文档任何人都检索不到；retrieval 在过滤与返回前复核两处读取。
CREATE TABLE idx.deny_list (
    doc_id     text PRIMARY KEY,
    reason     text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
