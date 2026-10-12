-- 学习型稀疏向量（retrieval 详设 6.2 的 sparse 召回路）：与稠密向量出自同一次嵌入调用，嵌入模型不给时为空。
-- 维数是 bge-m3 的词表大小；按内积排序（<#>）。与稠密向量一样，数据量小时按精确距离排序，量上来后按 retrieval 详设 5.4 加近似索引。
ALTER TABLE idx.chunks ADD COLUMN sparse sparsevec(250002);
