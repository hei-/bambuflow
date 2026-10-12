-- 全量重建期间的双写（入库管道详设第 10 节第 3 步）：重建的待发布版本（新模型）与增量的待发布版本（线上模型）可以同时存在，
-- 同一组嵌入模型与分块器仍只有一个待发布版本；写入按请求的模型落到对应的那个
DROP INDEX idx.index_versions_one_pending;
CREATE UNIQUE INDEX index_versions_one_pending ON idx.index_versions (embedding_model, chunker_version) WHERE status = 'pending';

-- 当前内容（content_hash）第一次写入时的内容版本：此后同一内容按另一个模型写入时另起一个内容版本，
-- 再按某个模型写入时复用其中块都是这个模型的那一个，不来回新建版本
ALTER TABLE idx.documents ADD COLUMN content_since bigint NOT NULL DEFAULT 0;
UPDATE idx.documents SET content_since = doc_version;
