import { Button, Card, Form, Input, Select, Space, Table, Tag, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type { EmergencyDenial, EmergencyDenialList } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const scopeText = { source: "数据源", document: "文档", tenant: "租户" } as const;

// 紧急拒绝（技术设计文档 8.x）：按数据源、文档或租户拒绝，只能新增，1 分钟内在召回与返回前复核两处生效
export function DenialsPage() {
  const { session } = useConsole();
  const list = useLoad(
    (signal) => request<EmergencyDenialList>(session, "/v1/admin/emergency-denials", { signal }),
    "denials",
  );
  const [form] = Form.useForm<{ scope: EmergencyDenial["scope"]; target: string; reason: string }>();
  const [busy, setBusy] = useState(false);
  const keyRef = useRef(newIdempotencyKey());
  async function add() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      await request(session, "/v1/admin/emergency-denials", { body: v, idempotencyKey: keyRef.current });
      void message.success("已添加，1 分钟内生效");
      keyRef.current = newIdempotencyKey();
      form.resetFields();
      list.reload();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card title="新增紧急拒绝（只能新增，不能撤销）">
        <Form form={form} layout="inline" initialValues={{ scope: "document" }}>
          <Form.Item name="scope" rules={[{ required: true }]}>
            <Select
              style={{ width: 120 }}
              options={Object.entries(scopeText).map(([value, label]) => ({ value, label }))}
            />
          </Form.Item>
          <Form.Item name="target" rules={[{ required: true, whitespace: true, message: "请填写对象 ID" }]}>
            <Input placeholder="doc_id / src_… / 租户" style={{ width: 240 }} />
          </Form.Item>
          <Form.Item name="reason" rules={[{ required: true, whitespace: true, max: 500, message: "请填写原因" }]}>
            <Input placeholder="原因" style={{ width: 320 }} />
          </Form.Item>
          <Button type="primary" danger loading={busy} onClick={() => void add()}>
            拒绝
          </Button>
        </Form>
      </Card>
      <Card title="已有规则">
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <Table<EmergencyDenial>
            rowKey="denial_id"
            size="small"
            loading={list.loading}
            dataSource={list.data?.denials ?? []}
            locale={{ emptyText: "没有紧急拒绝规则" }}
            columns={[
              { title: "范围", dataIndex: "scope", render: (s: EmergencyDenial["scope"]) => scopeText[s] },
              { title: "对象", dataIndex: "target" },
              { title: "原因", dataIndex: "reason" },
              {
                title: "状态",
                dataIndex: "status",
                render: (s: string) => (
                  <Tag color={s === "active" ? "red" : "blue"}>{s === "active" ? "已生效" : "生效中"}</Tag>
                ),
              },
              { title: "添加人", dataIndex: "created_by" },
              { title: "时间", dataIndex: "created_at", render: fmtTime },
            ]}
          />
        )}
      </Card>
    </Space>
  );
}
