import { Button, Card, DatePicker, Form, Input, Select, Space, Table, Tag } from "antd";
import dayjs, { type Dayjs } from "dayjs";
import { useState } from "react";
import { request } from "../api/client";
import type { AuditRecord, AuditRecordList } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

interface Query {
  sub?: string;
  doc_id?: string;
  kind?: AuditRecord["kind"];
  from: string;
  to: string;
}

const kindLabel: Record<AuditRecord["kind"], string> = {
  access: "线上访问",
  change: "变更",
  eval: "评测回放",
  restricted: "受限级访问",
};
const kindColor: Partial<Record<AuditRecord["kind"], string>> = { change: "blue", eval: "purple", restricted: "red" };

// 审计查询（只有审计岗；查询本身也写审计）：按用户、文档、时间查谁在什么时候看到了什么、改了什么。
// 走审计索引，时间跨度不限；不选类别时查线上访问与变更，评测回放要单独选
export function AuditPage() {
  const { session } = useConsole();
  const [form] = Form.useForm<{ sub?: string; doc_id?: string; kind?: AuditRecord["kind"]; range: [Dayjs, Dayjs] }>();
  const [query, setQuery] = useState<Query>(() => ({
    from: dayjs().subtract(1, "day").toISOString(),
    to: dayjs().toISOString(),
  }));
  const [pages, setPages] = useState<string[]>([""]);
  const token = pages[pages.length - 1];
  const list = useLoad(
    (signal) =>
      request<AuditRecordList>(session, "/v1/admin/audit", {
        query: { ...query, page_size: 50, page_token: token },
        signal,
      }),
    JSON.stringify([query, token]),
  );
  function search() {
    const v = form.getFieldsValue();
    setQuery({
      sub: v.sub?.trim() || undefined,
      doc_id: v.doc_id?.trim() || undefined,
      kind: v.kind,
      from: v.range[0].toISOString(),
      to: v.range[1].toISOString(),
    });
    setPages([""]);
  }
  return (
    <Card title="审计查询">
      <Form
        form={form}
        layout="inline"
        initialValues={{ range: [dayjs().subtract(1, "day"), dayjs()] }}
        onFinish={search}
      >
        <Form.Item name="sub" label="用户">
          <Input placeholder="用户名" allowClear />
        </Form.Item>
        <Form.Item name="doc_id" label="文档">
          <Input placeholder="doc_id" allowClear />
        </Form.Item>
        <Form.Item name="kind" label="类别">
          <Select
            allowClear
            placeholder="线上访问与变更"
            style={{ width: 160 }}
            options={Object.entries(kindLabel).map(([value, label]) => ({ value, label }))}
          />
        </Form.Item>
        <Form.Item name="range" label="时间" rules={[{ required: true }]}>
          <DatePicker.RangePicker showTime />
        </Form.Item>
        <Button type="primary" htmlType="submit">
          查询
        </Button>
      </Form>
      <div style={{ marginTop: 16 }}>
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <>
            <Table<AuditRecord>
              rowKey={(r) =>
                `${r.kind}-${r.trace_id}-${r.action ?? ""}-${r.time}-${r.shown_chunk_ids.join(",")}-${r.listed_doc_ids.join(",")}`
              }
              size="small"
              loading={list.loading}
              dataSource={list.data?.records ?? []}
              pagination={false}
              locale={{ emptyText: "这段时间没有匹配的记录" }}
              expandable={{
                expandedRowRender: (r) =>
                  r.kind === "change" ? (
                    <Space direction="vertical">
                      <span>角色：{r.roles?.join("、") || "—"}</span>
                      <span>依据：{r.detail ? JSON.stringify(r.detail) : "—"}</span>
                      <span>追踪 ID：{r.trace_id || "—"}</span>
                    </Space>
                  ) : (
                    <Space direction="vertical">
                      {r.gate_id && <span>门禁：{r.gate_id}</span>}
                      <span>候选块：{r.candidate_chunk_ids.join("、") || "—"}</span>
                      <span>进入上下文：{r.context_chunk_ids.join("、") || "—"}</span>
                      <span>展示给用户：{r.shown_chunk_ids.join("、") || "—"}</span>
                      <span>列出的文档：{r.listed_doc_ids.join("、") || "—"}</span>
                      {r.answer_status && (
                        <span>
                          回答状态：{r.answer_status}；回答摘要：{r.answer_sha256 ?? "—"}
                        </span>
                      )}
                      {r.versions && (
                        <span>
                          版本：索引 {r.versions.index_version ?? "—"} · 策略 {r.versions.policy_version ?? "—"} ·
                          生成模型 {r.versions.generation_model ?? "—"} · 提示词 {r.versions.prompt_version ?? "—"}
                          {r.versions.degraded?.length ? ` · 降级 ${r.versions.degraded.join("、")}` : ""}
                        </span>
                      )}
                      <span>追踪 ID：{r.trace_id}</span>
                    </Space>
                  ),
              }}
              columns={[
                { title: "时间", dataIndex: "time", render: fmtTime },
                {
                  title: "类别",
                  render: (_, r) => (
                    <Tag color={kindColor[r.kind]}>
                      {kindLabel[r.kind]}·{r.source}
                    </Tag>
                  ),
                },
                { title: "用户", dataIndex: "sub" },
                { title: "租户", dataIndex: "tenant" },
                { title: "客户端", dataIndex: "actor" },
                {
                  title: "接口或动作",
                  render: (_, r) =>
                    r.kind === "change"
                      ? `${r.action ?? ""} ${r.object_id ?? ""}`
                      : r.admin_path
                        ? `${r.admin_method} ${r.admin_path}`
                        : r.endpoint,
                },
                {
                  title: "状态",
                  render: (_, r) =>
                    r.kind === "change" ? "—" : <Tag color={r.status < 400 ? "green" : "red"}>{r.status}</Tag>,
                },
                { title: "命中位置", dataIndex: "matched_in", render: (m?: string[]) => m?.join("、") ?? "—" },
                {
                  title: "耗时",
                  render: (_, r) => (r.kind === "change" ? "—" : `${r.latency_ms} ms`),
                },
              ]}
            />
            <Space style={{ marginTop: 8 }}>
              <Button disabled={pages.length <= 1} onClick={() => setPages((p) => p.slice(0, -1))}>
                上一页
              </Button>
              <Button
                disabled={!list.data?.next_page_token}
                onClick={() => list.data?.next_page_token && setPages((p) => [...p, list.data!.next_page_token!])}
              >
                下一页
              </Button>
            </Space>
          </>
        )}
      </div>
    </Card>
  );
}
