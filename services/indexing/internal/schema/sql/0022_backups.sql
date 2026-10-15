-- 灾备备份（总体设计 11.5，部署配置清单 P2）：每小时把线上版本备份到跨地域复制的对象存储，按文档加密。
-- 每份文档一把备份数据密钥，经密钥服务包裹后存这里；物理删除时删掉这一行（销毁密钥），备份里这份文档的内容就再也解不开。
-- 备份桶里的密钥环按 shard（doc_id 的 SHA-256 首字节）分 256 片，只重写有变化的分片。
CREATE TABLE idx.backup_keys (
    doc_id     text PRIMARY KEY,
    shard      smallint    NOT NULL CHECK (shard BETWEEN 0 AND 255),
    wrapped    text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- 要重写的密钥环分片：新增或销毁密钥时标记（marked_at 取最近一次），备份任务重写后清掉不晚于读取时刻的标记
CREATE TABLE idx.backup_keyring_dirty (
    shard     smallint PRIMARY KEY,
    marked_at timestamptz NOT NULL DEFAULT now()
);

-- 已写进备份桶的文档对象（每个内容版本一个）。last_ref 是最近一次被快照引用的时间，早于保留的最老快照的对象可以删
CREATE TABLE idx.backup_objects (
    doc_id      text        NOT NULL,
    doc_version bigint      NOT NULL,
    written_at  timestamptz NOT NULL,
    last_ref    timestamptz NOT NULL,
    PRIMARY KEY (doc_id, doc_version)
);
CREATE INDEX backup_objects_last_ref ON idx.backup_objects (last_ref);

-- 物理删除时销毁的备份密钥：备份桶里对应的密钥环分片重写后，补记进删除凭证（backups）
CREATE TABLE idx.backup_shreds (
    receipt_id   text PRIMARY KEY,
    shard        smallint    NOT NULL,
    destroyed_at timestamptz NOT NULL DEFAULT now()
);
