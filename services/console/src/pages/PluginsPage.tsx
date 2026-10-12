import { PlusOutlined } from "@ant-design/icons";
import { Button, Card, Form, Input, Modal, Select, Space, Table, Tag, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type { Plugin, PluginList } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { Pager, usePages } from "../components/Pager";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const kindLabel: Record<Plugin["kind"], string> = {
  connector: "连接器",
  parser: "解析器",
  chunker: "分块器",
  retriever: "召回",
  reranker: "精排",
  model: "模型",
  tool: "Tool",
};

const reviewTag: Record<Plugin["security_review"], { label: string; color: string }> = {
  not_required: { label: "不需要", color: "default" },
  pending: { label: "待评审", color: "orange" },
  approved: { label: "已通过", color: "green" },
  rejected: { label: "未通过", color: "red" },
};

// 插件登记（governance 详设 3.1）：平台管理员登记；各租户的评估结果由 CI 写入，通过评估的租户才能在 pipeline.yaml 里引用；
// Tool 插件另要信息安全评审（登记人不能评审）
export function PluginsPage() {
  const { session } = useConsole();
  const pages = usePages();
  const list = useLoad(
    (signal) =>
      request<PluginList>(session, "/v1/admin/plugins", { query: { page_size: 50, page_token: pages.token }, signal }),
    pages.token ?? "",
  );
  return (
    <Card title="插件" extra={<RegisterButton onDone={list.reload} />}>
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <>
          <Table<Plugin>
            rowKey="plugin_id"
            size="small"
            loading={list.loading}
            dataSource={list.data?.plugins ?? []}
            pagination={false}
            locale={{ emptyText: "还没有登记插件" }}
            expandable={{
              expandedRowRender: (p) => (
                <Space direction="vertical">
                  <span>说明：{p.description}</span>
                  {p.tool_scopes.length > 0 && <span>访问范围：{p.tool_scopes.join("、")}</span>}
                  {p.reviewed_by && (
                    <span>
                      评审：{p.reviewed_by}，{fmtTime(p.reviewed_at)}，{p.review_note}
                    </span>
                  )}
                  <span>
                    评估：
                    {p.evaluations.length === 0
                      ? "还没有评估结果"
                      : p.evaluations
                          .map((e) => `${e.tenant} ${e.passed ? "通过" : "未通过"}（${fmtTime(e.evaluated_at)}）`)
                          .join("；")}
                  </span>
                </Space>
              ),
            }}
            columns={[
              { title: "插件", render: (_, p) => `${p.name}@${p.version}` },
              { title: "类型", dataIndex: "kind", render: (k: Plugin["kind"]) => kindLabel[k] },
              { title: "宿主", dataIndex: "host" },
              {
                title: "安全评审",
                dataIndex: "security_review",
                render: (r: Plugin["security_review"]) => <Tag color={reviewTag[r].color}>{reviewTag[r].label}</Tag>,
              },
              {
                title: "可引用的租户",
                dataIndex: "usable_tenants",
                render: (t: string[]) => t.join("、") || "—",
              },
              { title: "登记", render: (_, p) => `${p.registered_by}，${fmtTime(p.registered_at)}` },
              {
                title: "",
                render: (_, p) =>
                  p.security_review === "pending" ? <ReviewActions plugin={p} onDone={list.reload} /> : null,
              },
            ]}
          />
          <Pager pages={pages} nextToken={list.data?.next_page_token} />
        </>
      )}
    </Card>
  );
}

function ReviewActions({ plugin, onDone }: { plugin: Plugin; onDone: () => void }) {
  const { session } = useConsole();
  const review = (approved: boolean) => async (note: string, key: string) => {
    await request<Plugin>(session, `/v1/admin/plugins/${plugin.plugin_id}/security-review`, {
      body: { approved, note },
      idempotencyKey: key,
    });
    return approved ? "评审通过" : "评审未通过";
  };
  return (
    <Space>
      <ReasonAction
        label="评审通过"
        title={`信息安全评审 ${plugin.name}@${plugin.version}：通过`}
        fieldLabel="评审意见"
        onSubmit={review(true)}
        onDone={onDone}
      />
      <ReasonAction
        label="不通过"
        title={`信息安全评审 ${plugin.name}@${plugin.version}：不通过`}
        fieldLabel="评审意见"
        danger
        onSubmit={review(false)}
        onDone={onDone}
      />
    </Space>
  );
}

interface RegisterForm {
  kind: Plugin["kind"];
  name: string;
  version: string;
  description: string;
  tool_scopes?: string[];
}

function RegisterButton({ onDone }: { onDone: () => void }) {
  const { session } = useConsole();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<RegisterForm>();
  const kind = Form.useWatch("kind", form);
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      const p = await request<Plugin>(session, "/v1/admin/plugins", {
        body: {
          kind: v.kind,
          name: v.name.trim(),
          version: v.version.trim(),
          description: v.description.trim(),
          tool_scopes: v.kind === "tool" ? v.tool_scopes : undefined,
        },
        idempotencyKey: keyRef.current,
      });
      keyRef.current = newIdempotencyKey();
      void message.success(
        p.security_review === "pending" ? `已登记 ${p.plugin_id}，待信息安全评审` : `已登记 ${p.plugin_id}`,
      );
      setOpen(false);
      form.resetFields();
      onDone();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      <Button size="small" icon={<PlusOutlined />} onClick={() => setOpen(true)}>
        登记插件
      </Button>
      <Modal
        open={open}
        title="登记插件版本"
        okText="登记"
        cancelText="取消"
        confirmLoading={busy}
        onOk={() => void submit()}
        onCancel={() => setOpen(false)}
      >
        <Form form={form} layout="vertical" initialValues={{ kind: "parser" }}>
          <Form.Item name="kind" label="类型" rules={[{ required: true }]}>
            <Select options={Object.entries(kindLabel).map(([value, label]) => ({ value, label }))} />
          </Form.Item>
          <Form.Item
            name="name"
            label="名称"
            rules={[
              {
                required: true,
                pattern: /^[a-z][a-z0-9-]{1,63}$/,
                message: "小写字母开头，只含小写字母、数字和连字符",
              },
            ]}
          >
            <Input />
          </Form.Item>
          <Form.Item name="version" label="版本" rules={[{ required: true, whitespace: true, max: 64 }]}>
            <Input placeholder="如 1.2.0" />
          </Form.Item>
          <Form.Item name="description" label="说明" rules={[{ required: true, whitespace: true, max: 500 }]}>
            <Input.TextArea autoSize={{ minRows: 2 }} maxLength={500} showCount />
          </Form.Item>
          {kind === "tool" && (
            <Form.Item
              name="tool_scopes"
              label="访问范围"
              extra="只能是只读范围（read: 开头），回车分隔"
              rules={[
                { required: true, message: "Tool 插件须填写访问范围" },
                {
                  validator: (_, v?: string[]) =>
                    (v ?? []).every((s) => s.startsWith("read:"))
                      ? Promise.resolve()
                      : Promise.reject(new Error("只能是 read: 开头的只读范围")),
                },
              ]}
            >
              <Select mode="tags" open={false} maxCount={20} />
            </Form.Item>
          )}
        </Form>
      </Modal>
    </>
  );
}
