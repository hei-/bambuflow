-- 图谱（P3-D，技术设计文档 6.3、12.7 的开发测试候选“PostgreSQL 实体表 + 边表”）：只有 indexing 写，retrieval 只读。
--
-- 关系按“产生它的文档内容版本”存放（doc_id, doc_version），不单独做版本管理：
-- 某个索引版本里可见的边 = 该版本清单（version_docs）列出的文档版本产生的边，
-- 所以增量发布带入、回滚都跟着版本清单走；权限、密级、删除标记与拒绝名单在文档级、取最新值，
-- 和块一样在召回时按证据块所属文档过滤。删除标记打上后只依赖这份文档证据的边随即不可见，物理删除随下次全量合并。

-- 实体只有类型和名称，不带任何文档信息；名称只有在证据块可见的边上才会返回给调用者。
CREATE TABLE idx.entities (
    -- 类型 + 归一化名称（如 system:清算核心服务），同一实体在不同文档里合并成一个
    entity_id      text PRIMARY KEY,
    -- bambuflow.common.v1.EntityType 的编号
    type           smallint    NOT NULL CHECK (type BETWEEN 1 AND 5),
    canonical_name text        NOT NULL,
    -- 原文里的其他说法，实体链接时与规范名称一起匹配
    aliases        text[]      NOT NULL DEFAULT '{}',
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX entities_type_name ON idx.entities (type, canonical_name);

-- 一行是一条关系的一个证据块。同一关系被多份文档证明时有多行，任一证据块可见即可沿这条边走。
CREATE TABLE idx.relations (
    doc_id            text     NOT NULL REFERENCES idx.documents (doc_id),
    doc_version       bigint   NOT NULL,
    from_entity       text     NOT NULL REFERENCES idx.entities (entity_id),
    -- bambuflow.common.v1.RelationType 的编号
    relation_type     smallint NOT NULL CHECK (relation_type BETWEEN 1 AND 5),
    to_entity         text     NOT NULL REFERENCES idx.entities (entity_id),
    evidence_chunk_id text     NOT NULL,
    confidence        real     NOT NULL CHECK (confidence > 0 AND confidence <= 1),
    -- 抽检不合格时按它暂停发布某一抽取器版本的关系（入库管道详设第 7 节）
    extractor_version text     NOT NULL,
    PRIMARY KEY (doc_id, doc_version, from_entity, relation_type, to_entity, evidence_chunk_id)
);
CREATE INDEX relations_from ON idx.relations (from_entity, relation_type);
CREATE INDEX relations_to ON idx.relations (to_entity, relation_type);
CREATE INDEX relations_evidence ON idx.relations (evidence_chunk_id);
