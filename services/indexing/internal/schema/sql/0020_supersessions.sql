-- 替代关系（入库管道详设第 7 节效力信息，评估报告 K3）：新文档声明替代了哪些文档，indexing 在同一数据源里关联被替代的文档，
-- 只改文档级字段（superseded_by、validity_status），不重新向量化。
ALTER TABLE idx.documents
    ADD COLUMN superseded_by text NOT NULL DEFAULT '',
    -- 规范化标题（去掉末尾的版本、年份括注）：按“《标题》同时废止”关联时用
    ADD COLUMN title_key     text NOT NULL DEFAULT '';
CREATE INDEX documents_title_key ON idx.documents (source_id, title_key) WHERE title_key <> '';

-- 声明：by_doc_id 替代 target（doc:<doc_id> 或 title:<规范化标题>）。新文档写入时整份替换它的声明；
-- 被替代的文档晚于新文档入库时，也按这张表反向关联
CREATE TABLE idx.supersessions (
    source_id text NOT NULL,
    by_doc_id text NOT NULL,
    target    text NOT NULL,
    PRIMARY KEY (source_id, by_doc_id, target)
);
CREATE INDEX supersessions_target ON idx.supersessions (source_id, target);
