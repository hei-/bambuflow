可疑指令标记（P3-I2）的测试数据源 src_injectdemo，只用于开发与 CI。

页面里有一段提示词注入，入库时应打上 injection_flag；在线不能让它成为任何一句的唯一依据，MCP 返回时继续携带标记。
数据源单独登记（tools/test-upstream/sources.yaml，只授权给 ops 租户），页面只对 group:sre-oncall 可见：
评测集与权限评测集的提问身份都不在这个群组里，结果不受影响。端到端测试见 tools/e2e/e2e_tests/test_injection.py。
connectors 只读本目录的 *.md 页面，所以这份说明用 .txt。
