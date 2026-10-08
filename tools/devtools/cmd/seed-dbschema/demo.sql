-- 关系型数据库数据源的演示库（P3-I4，只用于开发与 CI）：放在开发 Postgres 的 bankdemo schema 里，可重复执行。
-- 两个视图在白名单里（tools/test-upstream/dbschema.yaml），v_customer_contact 不在；
-- 行数据只用来证明“行数据不入索引”，内容都是虚构的。
CREATE SCHEMA IF NOT EXISTS bankdemo;

CREATE TABLE IF NOT EXISTS bankdemo.loan (
    loan_id   bigint PRIMARY KEY,
    branch    text          NOT NULL,
    balance   numeric(18, 2) NOT NULL,
    opened_on date          NOT NULL
);
CREATE TABLE IF NOT EXISTS bankdemo.customer (
    customer_id bigint PRIMARY KEY,
    name        text NOT NULL,
    phone       text NOT NULL
);
INSERT INTO bankdemo.loan VALUES (900001, '演示网点甲', 765432.10, '2025-03-01'), (900002, '演示网点乙', 12345.67, '2025-06-15')
ON CONFLICT DO NOTHING;
INSERT INTO bankdemo.customer VALUES (800001, '演示客户甲', '00000000000') ON CONFLICT DO NOTHING;

CREATE OR REPLACE VIEW bankdemo.v_loan_balance AS
    SELECT loan_id, branch, balance, opened_on FROM bankdemo.loan;
COMMENT ON VIEW bankdemo.v_loan_balance IS '贷款余额视图：每笔贷款的当前余额，按日更新，供运营报表查询。';
COMMENT ON COLUMN bankdemo.v_loan_balance.loan_id IS '贷款编号';
COMMENT ON COLUMN bankdemo.v_loan_balance.branch IS '经办网点';
COMMENT ON COLUMN bankdemo.v_loan_balance.balance IS '贷款余额（元）';
COMMENT ON COLUMN bankdemo.v_loan_balance.opened_on IS '放款日期';

CREATE OR REPLACE VIEW bankdemo.v_branch_daily AS
    SELECT branch, count(*) AS loan_count, sum(balance) AS total_balance FROM bankdemo.loan GROUP BY branch;
COMMENT ON VIEW bankdemo.v_branch_daily IS '网点贷款日汇总视图：按网点统计贷款笔数与余额合计。';
COMMENT ON COLUMN bankdemo.v_branch_daily.branch IS '网点';
COMMENT ON COLUMN bankdemo.v_branch_daily.loan_count IS '贷款笔数';
COMMENT ON COLUMN bankdemo.v_branch_daily.total_balance IS '余额合计（元）';

CREATE OR REPLACE VIEW bankdemo.v_customer_contact AS
    SELECT customer_id, name, phone FROM bankdemo.customer;
COMMENT ON VIEW bankdemo.v_customer_contact IS '客户联系方式（不在白名单里，不应同步）。';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_ops') THEN
        CREATE ROLE report_ops NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_finance') THEN
        CREATE ROLE report_finance NOLOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA bankdemo TO report_ops, report_finance;
GRANT SELECT ON bankdemo.v_loan_balance TO report_ops;
GRANT SELECT ON bankdemo.v_branch_daily TO report_ops, report_finance;
GRANT SELECT ON bankdemo.v_customer_contact TO report_finance;
