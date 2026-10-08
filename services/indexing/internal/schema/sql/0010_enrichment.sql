-- 增强产出的摘要与关键词（P3-I1，入库管道详设第 7 节）。
-- 文档级摘要单独存为摘要块：和正文块在同一张表里，权限、密级、删除标记随所属文档（doc_acl），引用、复核、
-- 取片段的路径都不用改；kind 区分两者，普通召回只取正文块（K2 详设第 3 节：摘要向量与正文分开）。
ALTER TABLE idx.chunks
    ADD COLUMN kind text NOT NULL DEFAULT 'content' CHECK (kind IN ('content', 'summary'));
-- 每个内容版本至多一个摘要块
CREATE UNIQUE INDEX chunks_one_summary ON idx.chunks (doc_id, doc_version) WHERE kind = 'summary';

-- 文档最新内容版本的摘要块与关键词（技术设计文档 4.1 documents.summary_chunk_id）。和标题一样取最新值；
-- 按索引版本读取时以版本清单里的 doc_version 找摘要块（retrieval），回滚后不会指到不在线上的块。
ALTER TABLE idx.documents
    ADD COLUMN summary_chunk_id text   NOT NULL DEFAULT '',
    ADD COLUMN keywords         text[] NOT NULL DEFAULT '{}';

-- 章节级摘要与关键词：按内容版本保存，与块的保留方式一致（回滚时随旧正文一起可用）。
CREATE TABLE idx.sections (
    doc_id          text   NOT NULL REFERENCES idx.documents (doc_id),
    doc_version     bigint NOT NULL,
    parent_chunk_id text   NOT NULL,
    heading_path    text[] NOT NULL,
    summary         text   NOT NULL,
    keywords        text[] NOT NULL,
    PRIMARY KEY (doc_id, doc_version, parent_chunk_id)
);
