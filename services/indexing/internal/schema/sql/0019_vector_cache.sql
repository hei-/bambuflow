-- 按块的向量缓存（入库管道详设第 8 节）：块记录本来就存着向量，再记向量化输入（标题路径 + 文本）的摘要，
-- processing 向量化前按（嵌入模型、输入摘要）查已有的向量，命中的不再送嵌入模型。不另建缓存表：
-- 块记录随保留的索引版本一起清理，缓存的生命周期与之一致；向量与原文在同一处存放，密级保护不变。
ALTER TABLE idx.chunks ADD COLUMN input_hash text NOT NULL DEFAULT '';
CREATE INDEX chunks_vector_cache ON idx.chunks (embedding_model, input_hash) WHERE input_hash <> '';
