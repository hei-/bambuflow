# ops 租户（开发与测试）：评测用的信贷数据源虽已授权，策略上不让运营检索（权限评测集据此出越权题）。
deny_sources := {"src_evalcredit"}

test_eval_source_denied if "src_evalcredit" in deny_sources
