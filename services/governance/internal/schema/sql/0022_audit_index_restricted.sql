-- 审计索引新增 restricted 类：gateway 为受限级内容单独写的访问记录（技术设计文档 8.6“每次访问单独审计”）
ALTER TABLE governance.audit_index DROP CONSTRAINT audit_index_kind_check;
ALTER TABLE governance.audit_index ADD CONSTRAINT audit_index_kind_check
    CHECK (kind IN ('access', 'change', 'eval', 'restricted'));
