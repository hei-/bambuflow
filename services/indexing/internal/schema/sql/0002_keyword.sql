-- 关键词检索（P1-T3）：PostgreSQL 全文检索，分词在应用侧完成（libs/textseg），这里只存切好的词。
-- 用 'simple' 配置：词已切好，不再做词干化；权重 A 标题、B 标题路径、C 正文（retrieval 详设 6.2）。
ALTER TABLE idx.chunks ADD COLUMN tsv tsvector;
CREATE INDEX chunks_tsv ON idx.chunks USING gin (tsv);
