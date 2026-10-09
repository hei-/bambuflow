-- 删除覆盖全部副本与删除凭证（G4，技术设计文档 9.2，评估报告 K6）：删除标记写下后先保留一段观察期（可以回滚、可以撤销），
-- 过了观察期由 indexing purge-deleted 物理删除索引侧的全部副本，并为每份文档留一份删除凭证，记各存储完成的时间。
-- 删除标记本身（doc_acl 的 tombstoned 行与 documents 的最小信息）保留：删除优先要靠它挡住更早产生的内容写入。
ALTER TABLE idx.doc_acl ADD COLUMN purged_at timestamptz;
CREATE INDEX doc_acl_purge_due ON idx.doc_acl (tombstoned_at) WHERE state = 'tombstoned' AND purged_at IS NULL;

CREATE TABLE idx.deletion_receipts (
    receipt_id    text        PRIMARY KEY,
    doc_id        text        NOT NULL,
    source_id     text        NOT NULL,
    tombstoned_at timestamptz,
    -- 存储 → 完成时间（chunks 含关键词与向量索引、sections、graph、manifests、metadata）
    stores        jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deletion_receipts_doc ON idx.deletion_receipts (doc_id);
