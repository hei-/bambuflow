import { Alert, Button, Card, Input, Space, Table, Tag, Typography, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type { IndexVersion, IndexVersionList, Operation } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const statusColor = { live: "green", pending: "blue", retired: "default" } as const;
const statusText = { live: "线上", pending: "待发布", retired: "已下线" } as const;

// 索引版本（入库管道详设第 10 节）：切换或回滚要第二位平台运维复核后才执行
export function IndexVersionsPage() {
  const { session } = useConsole();
  const list = useLoad(
    (signal) => request<IndexVersionList>(session, "/v1/admin/index-versions", { signal }),
    "versions",
  );
  const [op, setOp] = useState<Operation>();
  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card title="索引版本">
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <Table<IndexVersion>
            rowKey="version"
            size="small"
            loading={list.loading}
            dataSource={list.data?.versions ?? []}
            pagination={false}
            locale={{ emptyText: "还没有索引版本，先入库并发布" }}
            columns={[
              { title: "版本", dataIndex: "version" },
              {
                title: "状态",
                dataIndex: "status",
                render: (s: IndexVersion["status"]) => <Tag color={statusColor[s]}>{statusText[s]}</Tag>,
              },
              { title: "嵌入模型", dataIndex: "embedding_model" },
              { title: "分块器", dataIndex: "chunker_version" },
              {
                title: "文档数",
                render: (_, v) => (v.missing_doc_count ? `${v.doc_count}（缺 ${v.missing_doc_count}）` : v.doc_count),
              },
              { title: "创建", dataIndex: "created_at", render: fmtTime },
              { title: "上线", dataIndex: "published_at", render: fmtTime },
              {
                title: "操作",
                render: (_, v) =>
                  v.status === "retired" ? (
                    <ReasonAction
                      label="回滚到此版本"
                      title={`回滚到 ${v.version}（需另一位平台运维复核）`}
                      danger
                      onSubmit={async (reason, key) => {
                        const r = await request<Operation>(
                          session,
                          `/v1/admin/index-versions/${encodeURIComponent(v.version)}/activate`,
                          {
                            body: { reason },
                            idempotencyKey: key,
                          },
                        );
                        setOp(r);
                        return `已创建待复核操作 ${r.operation_id}`;
                      }}
                    />
                  ) : null,
              },
            ]}
          />
        )}
      </Card>
      {op && (
        <Alert
          type="info"
          showIcon
          message={`待复核：${op.operation_id}`}
          description={`把这个操作 ID 交给另一位平台运维复核；${fmtTime(op.expires_at)} 前没有复核即过期。`}
        />
      )}
      <ApproveCard onDone={list.reload} />
    </Space>
  );
}

function ApproveCard({ onDone }: { onDone: () => void }) {
  const { session } = useConsole();
  const [id, setId] = useState("");
  const [busy, setBusy] = useState(false);
  const keyRef = useRef(newIdempotencyKey());
  async function approve() {
    setBusy(true);
    try {
      const r = await request<Operation>(
        session,
        `/v1/admin/index-versions/operations/${encodeURIComponent(id.trim())}/approve`,
        {
          method: "POST",
          idempotencyKey: keyRef.current,
        },
      );
      if (r.status === "executed") void message.success(`已切换到 ${r.target_version}`);
      else void message.warning(`操作状态：${r.status}${r.failure_reason ? `（${r.failure_reason}）` : ""}`);
      keyRef.current = newIdempotencyKey();
      setId("");
      onDone();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Card title="复核切换">
      <Typography.Paragraph type="secondary">发起人不能复核自己的操作。</Typography.Paragraph>
      <Space.Compact style={{ width: 480 }}>
        <Input placeholder="操作 ID（op_ 开头）" value={id} onChange={(e) => setId(e.target.value)} />
        <Button type="primary" loading={busy} disabled={!id.trim().startsWith("op_")} onClick={() => void approve()}>
          复核通过并执行
        </Button>
      </Space.Compact>
    </Card>
  );
}
