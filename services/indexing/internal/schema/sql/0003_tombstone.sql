-- 删除标记的时间（P2-T0）：“删除优先”用它判断一次内容写入是否早于删除（入库管道详设第 4 节）。
ALTER TABLE idx.doc_acl ADD COLUMN tombstoned_at timestamptz;
