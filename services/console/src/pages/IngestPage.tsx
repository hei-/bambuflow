import { Alert, Button, Card, Descriptions, Form, Input, Select, Space, Switch, Table, Tag } from "antd";
import { useState } from "react";
import { ApiError, request } from "../api/client";
import type { DeadLetter, DeadLetterList, IngestStatus, ReplayResult } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { Pager, usePages } from "../components/Pager";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const stages: DeadLetter["stage"][] = ["fetch", "parse", "mask", "enrich", "embed", "write"];

const stateLabel: Record<IngestStatus["state"], { label: string; color: string }> = {
  written: { label: "已写入", color: "green" },
  unchanged: { label: "内容未变", color: "green" },
  stale: { label: "已有更新的版本", color: "default" },
  deleted: { label: "已删除", color: "default" },
  acl_updated: { label: "已更新权限", color: "green" },
  retrying: { label: "重试中", color: "orange" },
  dead_letter: { label: "死信", color: "red" },
  quarantined: { label: "已隔离", color: "red" },
  queued: { label: "排队中", color: "blue" },
};

// 入库运维（入库管道详设 11、14.4，平台运维）：按文档查入库状态；死信与隔离文档的列表与重放。
// 重放只把死信里的原事件放回队列，不重新从源系统拉取；隔离的文档（重放 3 次仍失败）只有点名才重放
export function IngestPage() {
  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <StatusCard />
      <DeadLettersCard />
    </Space>
  );
}

function StatusCard() {
  const { session } = useConsole();
  const [doc, setDoc] = useState<string>();
  const st = useLoad(
    (signal) =>
      doc
        ? request<IngestStatus>(session, `/v1/admin/documents/${encodeURIComponent(doc)}/ingest-status`, { signal })
        : Promise.resolve(undefined),
    doc ?? "",
  );
  const notFound = st.error instanceof ApiError && st.error.status === 404;
  const s = st.data;
  return (
    <Card title="文档入库状态">
      <Input.Search
        style={{ width: 420 }}
        placeholder="文档 ID（引用预览里能看到）"
        enterButton="查询"
        allowClear
        onSearch={(v) => setDoc(v.trim() || undefined)}
      />
      <div style={{ marginTop: 16 }}>
        {notFound && <Alert type="info" showIcon message="没有这份文档的入库记录" />}
        {st.error !== undefined && !notFound && <ErrorAlert error={st.error} onRetry={st.reload} />}
        {doc && s && (
          <Descriptions size="small" column={3} bordered>
            <Descriptions.Item label="状态">
              <Tag color={stateLabel[s.state].color}>{stateLabel[s.state].label}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="阶段">{s.stage}</Descriptions.Item>
            <Descriptions.Item label="失败次数">{s.attempts}</Descriptions.Item>
            <Descriptions.Item label="数据源">{s.source_id}</Descriptions.Item>
            <Descriptions.Item label="事件">{s.event_id}</Descriptions.Item>
            <Descriptions.Item label="更新时间">{fmtTime(s.updated_at)}</Descriptions.Item>
            {s.last_error && (
              <Descriptions.Item label="最近失败" span={3}>
                {s.last_error.code}：{s.last_error.message}
              </Descriptions.Item>
            )}
            {s.ingest_batch_id && <Descriptions.Item label="重放批次">{s.ingest_batch_id}</Descriptions.Item>}
          </Descriptions>
        )}
      </div>
    </Card>
  );
}

interface Filter {
  source_id?: string;
  stage?: DeadLetter["stage"];
  error_code?: string;
  quarantined?: boolean;
}

function DeadLettersCard() {
  const { session } = useConsole();
  const [form] = Form.useForm<Filter>();
  const [filter, setFilter] = useState<Filter>({});
  const [selected, setSelected] = useState<string[]>([]);
  const pages = usePages();
  const list = useLoad(
    (signal) =>
      request<DeadLetterList>(session, "/v1/admin/dead-letters", {
        query: {
          ...filter,
          quarantined: filter.quarantined ? "true" : undefined,
          page_size: 50,
          page_token: pages.token,
        },
        signal,
      }),
    JSON.stringify([filter, pages.token]),
  );
  const reload = () => {
    setSelected([]);
    list.reload();
  };
  // 选中了就只重放选中的文档；没选按当前筛选条件重放（隔离的文档要点名）
  const replay = async (reason: string, key: string) => {
    const body = selected.length
      ? { doc_ids: selected, reason }
      : { source_id: filter.source_id, stage: filter.stage, error_code: filter.error_code, reason };
    const r = await request<ReplayResult>(session, "/v1/admin/replays", { body, idempotencyKey: key });
    if (!r.ingest_batch_id) return "没有符合条件的死信";
    const skipped = r.skipped_quarantined ? `，跳过隔离文档 ${r.skipped_quarantined} 份` : "";
    return `已放回 ${r.replayed} 份（批次 ${r.ingest_batch_id}）${skipped}`;
  };
  return (
    <Card
      title="死信与隔离文档"
      extra={
        <ReasonAction
          label={selected.length ? `重放选中的 ${selected.length} 份` : "按筛选条件重放"}
          title="把死信里的原事件放回入库队列"
          disabled={!selected.length && !filter.source_id && !filter.stage && !filter.error_code}
          onSubmit={replay}
          onDone={reload}
        />
      }
    >
      <Form
        form={form}
        layout="inline"
        onFinish={(v) => {
          pages.reset();
          setSelected([]);
          setFilter({
            source_id: v.source_id?.trim() || undefined,
            stage: v.stage,
            error_code: v.error_code?.trim() || undefined,
            quarantined: v.quarantined || undefined,
          });
        }}
      >
        <Form.Item name="source_id" label="数据源">
          <Input placeholder="src_ 开头" allowClear />
        </Form.Item>
        <Form.Item name="stage" label="阶段">
          <Select allowClear style={{ width: 120 }} options={stages.map((s) => ({ value: s, label: s }))} />
        </Form.Item>
        <Form.Item name="error_code" label="错误码">
          <Input allowClear />
        </Form.Item>
        <Form.Item name="quarantined" label="只看隔离" valuePropName="checked">
          <Switch size="small" />
        </Form.Item>
        <Button type="primary" htmlType="submit">
          筛选
        </Button>
      </Form>
      <div style={{ marginTop: 16 }}>
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <>
            <Table<DeadLetter>
              rowKey="doc_id"
              size="small"
              loading={list.loading}
              dataSource={list.data?.dead_letters ?? []}
              pagination={false}
              locale={{ emptyText: "没有死信" }}
              rowSelection={{ selectedRowKeys: selected, onChange: (keys) => setSelected(keys.map(String)) }}
              columns={[
                { title: "文档", dataIndex: "doc_id" },
                { title: "数据源", dataIndex: "source_id" },
                { title: "阶段", dataIndex: "stage" },
                { title: "错误", render: (_, d) => `${d.error.code}：${d.error.message}` },
                { title: "失败 / 重放", render: (_, d) => `${d.attempts} / ${d.replays}` },
                {
                  title: "",
                  dataIndex: "quarantined",
                  render: (q: boolean) => (q ? <Tag color="red">已隔离</Tag> : null),
                },
                { title: "最近失败", dataIndex: "last_failed_at", render: fmtTime },
              ]}
            />
            <Pager pages={pages} nextToken={list.data?.next_page_token} />
          </>
        )}
      </div>
    </Card>
  );
}
