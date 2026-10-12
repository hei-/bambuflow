# 共享盘样例源（只用于开发与 CI）

`fileshare/` 模拟公司共享盘上的制度文件与通知（P1-T2），数据源 ID `src_fileshare`，授权给 credit、risk、ops 三个测试租户
（`sources.yaml`）。内容均为虚构，主题与 Wiki 样例相关但不重复。

文件由 `gen_fileshare.py` 确定地生成（PyMuPDF、python-docx、openpyxl、标准库 email），合计不到 200 KB，随仓库提交，
这样 chunk_id 与评测题不随库版本漂移。每个文件旁挂 `<文件名>.meta.yaml`，模拟文档管理系统接口返回的 ID、属主、密级与 ACL，
字段与 Wiki 的 front matter 相同；connectors 只读取带元数据文件的文件，所以本说明放在目录外。

| 文件 | doc_id | 格式与处理方式 | 可见范围 | 密级 | 用途 |
| --- | --- | --- | --- | --- | --- |
| credit-rate-notice-2026q4.pdf | fs-credit-rate-notice | 文本 PDF，2 页，含有边框的表格、重复的页眉页脚 | group:credit-l2 | 内部 | 页码与区域、表头保留、页眉页脚去重 |
| branch-fire-inspection-scan.pdf | fs-fire-inspection | 扫描件 PDF（只有图片，没有文字层），走 OCR | group:all-staff | 内部 | OCR 路径；公开级的 eve 不可见 |
| it-asset-management.docx | fs-it-asset | Word，标题样式与表格 | group:all-staff | 公开 | 标题路径、表格 |
| risk-limit-monitor-2026q3.xlsx | fs-risk-limit-monitor | Excel，两个工作表，使用率是带百分比格式的数值 | group:risk-dept | 机密 | 取计算值、按格式还原百分数；信贷人员不可见 |
| aml-training-notice.eml | fs-aml-training-mail | 邮件，含签名、免责声明、引用的历史邮件与 CSV 附件 | group:credit-l2、group:risk-dept | 内部 | 去掉引用与签名、只留显示名、附件递归解析 |
| visitor-notice.png | fs-visitor-notice | 图片，走 OCR，一图一块 | group:all-staff | 公开 | 图片 OCR；全员可见 |

改内容后重新生成：

```
make run CMD='uv run --locked --all-packages python tools/test-upstream/gen_fileshare.py tools/test-upstream/fileshare' \
    OUT='tools/test-upstream/fileshare'
```
