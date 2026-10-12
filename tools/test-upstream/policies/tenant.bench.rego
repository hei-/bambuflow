# bench 租户（开发与测试）：在授权的数据源之外不再收紧。
deny_sources := set()

test_no_extra_restriction if count(deny_sources) == 0
