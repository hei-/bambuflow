-- PII 原值的信封加密（技术设计文档 8.6，入库管道详设第 6 节）：text 只存等长掩码后的正文，原值的密文与被主密钥
-- 包裹的数据密钥放在这里；只有策略允许看 PII 原文的调用者，retrieval 才解开还原。没有 PII 的块为 NULL。
ALTER TABLE idx.chunks
    ADD COLUMN pii_ciphertext bytea,
    ADD COLUMN pii_wrapped_key text,
    ADD COLUMN pii_key_id text;
