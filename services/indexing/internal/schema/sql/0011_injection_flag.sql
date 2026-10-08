-- 可疑指令标记（P3-I2，入库管道详设第 7 节，技术设计文档 4.1 chunks.injection_flag）：入库时检测到正文里有提示词注入的块。
-- 块照常可检索；retrieval 原样返回，orchestrator 不让它成为任何陈述的唯一依据。
ALTER TABLE idx.chunks ADD COLUMN injection_flag boolean NOT NULL DEFAULT false;
