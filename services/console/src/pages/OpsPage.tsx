import { Button, Card, Form, Input, Space, Table, Tag, Typography } from "antd";
import { useState } from "react";
import { request } from "../api/client";
import type { DeletionReceipt, DeletionReceiptList, InstanceSync, ServiceSync, SnapshotSync } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

// 运维状态：配置快照的分发情况（平台管理员、平台运维）与删除凭证（审计岗、信息安全）。
// 两块各自按权限显示，没有权限的那块提示无权
export function OpsPage() {
  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <SnapshotSyncCard />
      <ReceiptsCard />
    </Space>
  );
}

function unsynced(s: ServiceSync): number {
  return s.instances.filter((i) => !i.in_sync).length;
}

function SnapshotSyncCard() {
  const { session } = useConsole();
  const sync = useLoad((signal) => request<SnapshotSync>(session, "/v1/admin/snapshot-sync", { signal }), "sync");
  return (
    <Card
      title="配置快照分发"
      extra={
        <Button size="small" onClick={sync.reload}>
          刷新
        </Button>
      }
    >
      <Typography.Paragraph type="secondary">
        没跟上的实例在用旧快照：加载失败的看原因，长时间不恢复的重建实例。processing
        只在启动时加载词表，词表变更后滚动重启才跟上。
      </Typography.Paragraph>
      {sync.error !== undefined ? (
        <ErrorAlert error={sync.error} onRetry={sync.reload} />
      ) : (
        <Table<ServiceSync>
          rowKey="service"
          size="small"
          loading={sync.loading}
          dataSource={sync.data?.services ?? []}
          pagination={false}
          expandable={{
            expandedRowRender: (s) => (
              <Table<InstanceSync>
                rowKey="instance_id"
                size="small"
                dataSource={s.instances}
                pagination={false}
                locale={{ emptyText: "最近 24 小时没有实例上报" }}
                columns={[
                  { title: "实例", dataIndex: "instance_id" },
                  { title: "已加载版本", dataIndex: "snapshot_version" },
                  {
                    title: "状态",
                    render: (_, i) =>
                      i.in_sync ? (
                        <Tag color="green">已同步</Tag>
                      ) : (
                        <Tag color="red">{i.loaded ? "旧版本" : `加载失败：${i.reason ?? "未知"}`}</Tag>
                      ),
                  },
                  { title: "最近上报", dataIndex: "acked_at", render: fmtTime },
                ]}
              />
            ),
          }}
          columns={[
            { title: "服务", dataIndex: "service" },
            { title: "当前版本", dataIndex: "current_version", render: (v: string) => v || "读不到" },
            { title: "实例数", render: (_, s) => s.instances.length },
            {
              title: "没跟上",
              render: (_, s) => {
                const n = unsynced(s);
                return n === 0 ? <Tag color="green">0</Tag> : <Tag color="red">{n}</Tag>;
              },
            },
          ]}
        />
      )}
    </Card>
  );
}

function ReceiptsCard() {
  const { session } = useConsole();
  const [form] = Form.useForm<{ doc_id?: string; source_id?: string }>();
  const [query, setQuery] = useState<{ doc_id?: string; source_id?: string }>();
  const list = useLoad(
    (signal) =>
      query
        ? request<DeletionReceiptList>(session, "/v1/admin/deletion-receipts", {
            query: { ...query, page_size: 50 },
            signal,
          })
        : Promise.resolve(undefined),
    JSON.stringify(query ?? null),
  );
  return (
    <Card title="删除凭证">
      <Form
        form={form}
        layout="inline"
        onFinish={(v) => {
          const doc = v.doc_id?.trim();
          const src = v.source_id?.trim();
          if (doc || src) setQuery({ doc_id: doc || undefined, source_id: src || undefined });
        }}
      >
        <Form.Item name="doc_id" label="文档">
          <Input placeholder="doc_id" allowClear />
        </Form.Item>
        <Form.Item name="source_id" label="数据源">
          <Input placeholder="src_ 开头" allowClear />
        </Form.Item>
        <Button type="primary" htmlType="submit">
          查询
        </Button>
      </Form>
      <div style={{ marginTop: 16 }}>
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <Table<DeletionReceipt>
            rowKey="receipt_id"
            size="small"
            loading={query !== undefined && list.loading}
            dataSource={list.data?.receipts ?? []}
            pagination={false}
            locale={{ emptyText: query ? "没有删除凭证" : "按文档或数据源查询" }}
            expandable={{
              expandedRowRender: (r) => (
                <Space direction="vertical">
                  {r.stores.map((s) => (
                    <span key={s.store}>
                      {s.store}：{fmtTime(s.completed_at)}
                    </span>
                  ))}
                </Space>
              ),
            }}
            columns={[
              { title: "凭证", dataIndex: "receipt_id" },
              { title: "文档", dataIndex: "doc_id" },
              { title: "数据源", dataIndex: "source_id" },
              { title: "删除标记时间", dataIndex: "tombstoned_at", render: fmtTime },
              {
                title: "未完成",
                dataIndex: "pending_stores",
                render: (p: string[]) =>
                  p.length === 0 ? (
                    <Tag color="green">全部完成</Tag>
                  ) : (
                    p.map((s) => (
                      <Tag key={s} color="orange">
                        {s}
                      </Tag>
                    ))
                  ),
              },
              { title: "生成时间", dataIndex: "created_at", render: fmtTime },
            ]}
          />
        )}
      </div>
    </Card>
  );
}
