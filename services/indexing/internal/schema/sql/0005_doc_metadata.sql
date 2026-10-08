-- 文档级受控元数据与效力信息（入库管道详设第 7 节）：ListDocuments、CountDocuments 与检索按它筛选。
-- 和标题一样在文档级、取最新值；取不到的字段为空，并记在 metadata_missing 里，供 metadata_gaps 统计。
ALTER TABLE idx.documents
    ADD COLUMN doc_type           text     NOT NULL DEFAULT '',
    ADD COLUMN dept               text     NOT NULL DEFAULT '',
    ADD COLUMN topic_codes        text[]   NOT NULL DEFAULT '{}',
    ADD COLUMN effective_date     date,
    -- bambuflow.common.v1.ValidityStatus 的编号，0 为未知
    ADD COLUMN validity_status    smallint NOT NULL DEFAULT 0 CHECK (validity_status BETWEEN 0 AND 3),
    ADD COLUMN metadata_missing   text[]   NOT NULL DEFAULT '{doc_type,dept,topic_codes,effective_date,validity_status}',
    ADD COLUMN vocabulary_version text     NOT NULL DEFAULT '';

CREATE INDEX documents_doc_type ON idx.documents (doc_type);
CREATE INDEX documents_dept ON idx.documents (dept);
CREATE INDEX documents_effective_date ON idx.documents (effective_date);
CREATE INDEX documents_topic_codes ON idx.documents USING gin (topic_codes);
