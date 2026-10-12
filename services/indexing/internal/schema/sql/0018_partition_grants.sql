-- 分区级数据源的分区授权（总体设计 4.1 partition_grants、ADR-14）：文档只带分区 token（part:<数据源>/<分区>），
-- “谁能看哪个分区”存在这里，retrieval 编译过滤条件时按主体展开。由 indexing 的快速通道按 PartitionGrantChanged 整个分区替换，
-- 授权变化不改索引。
CREATE TABLE idx.partition_grants (
    source_id text NOT NULL,
    partition text NOT NULL,
    -- 被授权方：group:、dept:、user: 开头的 token
    grantee   text NOT NULL,
    PRIMARY KEY (source_id, partition, grantee)
);
-- retrieval 按主体 token 展开
CREATE INDEX partition_grants_grantee ON idx.partition_grants (grantee);

-- 每个分区最近一次授权变化的时间：乱序到达的旧事件不生效
CREATE TABLE idx.partition_grant_state (
    source_id   text        NOT NULL,
    partition   text        NOT NULL,
    occurred_at timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, partition)
);
