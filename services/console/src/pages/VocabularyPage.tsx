import { PlusOutlined } from "@ant-design/icons";
import { Button, Card, Form, Input, Modal, Select, Space, Table, Tabs, Tag, Typography, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type { VocabularyTerm, VocabularyTermList } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { useConsole } from "../context";
import { useLoad } from "../useLoad";

type Vocabulary = VocabularyTerm["vocabulary"];

const vocabularies: { key: Vocabulary; label: string }[] = [
  { key: "doc_type", label: "文档类型" },
  { key: "dept", label: "发文部门" },
  { key: "topic", label: "主题" },
];

const codePattern = /^[A-Z][A-Z0-9-]{1,63}$/;

// 受控词表（governance 详设 6.1）：文档类型、发文部门、主题。编码一旦使用就不删除、不改义，只能停用并指向替代编码；
// 变更随词表快照下发，processing 滚动重启后按新词表归一
export function VocabularyPage() {
  return (
    <Card title="受控词表">
      <Tabs items={vocabularies.map((v) => ({ key: v.key, label: v.label, children: <Terms vocabulary={v.key} /> }))} />
    </Card>
  );
}

function Terms({ vocabulary }: { vocabulary: Vocabulary }) {
  const { session } = useConsole();
  const base = `/v1/admin/vocabularies/${vocabulary}/terms`;
  const list = useLoad((signal) => request<VocabularyTermList>(session, base, { signal }), vocabulary);
  const [editing, setEditing] = useState<Partial<VocabularyTerm>>();
  const [deprecating, setDeprecating] = useState<VocabularyTerm>();
  const terms = list.data?.terms ?? [];
  if (list.error !== undefined) return <ErrorAlert error={list.error} onRetry={list.reload} />;
  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Space>
        <Button size="small" icon={<PlusOutlined />} onClick={() => setEditing({})}>
          新增词条
        </Button>
        <Typography.Text type="secondary">快照版本：{list.data?.snapshot_version ?? "—"}</Typography.Text>
      </Space>
      <Table<VocabularyTerm>
        rowKey="code"
        size="small"
        loading={list.loading}
        dataSource={terms}
        pagination={{ pageSize: 50, hideOnSinglePage: true }}
        locale={{ emptyText: "这个词表还没有词条" }}
        columns={[
          { title: "编码", dataIndex: "code" },
          { title: "名称", dataIndex: "name" },
          { title: "同义词", dataIndex: "synonyms", render: (s: string[]) => s.join("、") || "—" },
          { title: "上级", dataIndex: "parent", render: (p?: string) => p || "—" },
          { title: "维护部门", dataIndex: "owner", render: (o?: string) => o || "—" },
          {
            title: "状态",
            render: (_, t) =>
              t.status === "active" ? <Tag color="green">在用</Tag> : <Tag>已停用 → {t.replaced_by}</Tag>,
          },
          {
            title: "",
            render: (_, t) =>
              t.status === "active" ? (
                <Space>
                  <Button size="small" onClick={() => setEditing(t)}>
                    修改
                  </Button>
                  <Button size="small" danger onClick={() => setDeprecating(t)}>
                    停用
                  </Button>
                </Space>
              ) : null,
          },
        ]}
      />
      {editing && (
        <TermModal
          base={base}
          term={editing}
          parents={terms.filter((t) => t.status === "active" && t.code !== editing.code).map((t) => t.code)}
          onClose={() => setEditing(undefined)}
          onDone={list.reload}
        />
      )}
      {deprecating && (
        <DeprecateModal
          base={base}
          term={deprecating}
          targets={terms.filter((t) => t.status === "active" && t.code !== deprecating.code).map((t) => t.code)}
          onClose={() => setDeprecating(undefined)}
          onDone={list.reload}
        />
      )}
    </Space>
  );
}

interface TermForm {
  code: string;
  name: string;
  synonyms?: string[];
  parent?: string;
  owner?: string;
}

// 新增或修改：同一编码再提交即修改（名称、同义词、上级、维护部门），编码本身不能改
function TermModal({
  base,
  term,
  parents,
  onClose,
  onDone,
}: {
  base: string;
  term: Partial<VocabularyTerm>;
  parents: string[];
  onClose: () => void;
  onDone: () => void;
}) {
  const { session } = useConsole();
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<TermForm>();
  const keyRef = useRef(newIdempotencyKey());
  const isNew = term.code === undefined;
  async function submit() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      await request<VocabularyTerm>(session, base, {
        body: {
          code: v.code.trim(),
          name: v.name.trim(),
          synonyms: (v.synonyms ?? []).map((s) => s.trim()).filter(Boolean),
          parent: v.parent || undefined,
          owner: v.owner?.trim() || undefined,
        },
        idempotencyKey: keyRef.current,
      });
      void message.success(isNew ? "已新增" : "已修改");
      onClose();
      onDone();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      open
      title={isNew ? "新增词条" : `修改 ${term.code}`}
      okText="保存"
      cancelText="取消"
      confirmLoading={busy}
      onOk={() => void submit()}
      onCancel={onClose}
    >
      <Form form={form} layout="vertical" initialValues={term}>
        <Form.Item
          name="code"
          label="编码"
          extra="一旦使用就不删除、不改义"
          rules={[{ required: true, pattern: codePattern, message: "大写字母开头，只含大写字母、数字和连字符" }]}
        >
          <Input disabled={!isNew} placeholder="如 REG-LOAN" />
        </Form.Item>
        <Form.Item name="name" label="名称" rules={[{ required: true, whitespace: true, max: 100 }]}>
          <Input />
        </Form.Item>
        <Form.Item name="synonyms" label="同义词" extra="回车分隔，最多 50 个">
          <Select mode="tags" open={false} maxCount={50} />
        </Form.Item>
        <Form.Item name="parent" label="上级编码">
          <Select allowClear showSearch options={parents.map((c) => ({ value: c, label: c }))} />
        </Form.Item>
        <Form.Item name="owner" label="维护部门" rules={[{ max: 100 }]}>
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );
}

// 停用：必须指向一个在用的替代编码；重新标注完成前，查询旧编码和新编码都会命中
function DeprecateModal({
  base,
  term,
  targets,
  onClose,
  onDone,
}: {
  base: string;
  term: VocabularyTerm;
  targets: string[];
  onClose: () => void;
  onDone: () => void;
}) {
  const { session } = useConsole();
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<{ replaced_by: string; reason: string }>();
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      await request<VocabularyTerm>(session, `${base}/${encodeURIComponent(term.code)}/deprecate`, {
        body: { replaced_by: v.replaced_by, reason: v.reason.trim() },
        idempotencyKey: keyRef.current,
      });
      void message.success(`已停用 ${term.code}`);
      onClose();
      onDone();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      open
      title={`停用 ${term.code}（${term.name}）`}
      okText="停用"
      okButtonProps={{ danger: true }}
      cancelText="取消"
      confirmLoading={busy}
      onOk={() => void submit()}
      onCancel={onClose}
    >
      <Form form={form} layout="vertical">
        <Form.Item name="replaced_by" label="替代编码" rules={[{ required: true }]}>
          <Select showSearch options={targets.map((c) => ({ value: c, label: c }))} />
        </Form.Item>
        <Form.Item name="reason" label="原因" rules={[{ required: true, whitespace: true, max: 500 }]}>
          <Input.TextArea autoSize={{ minRows: 2 }} maxLength={500} showCount />
        </Form.Item>
      </Form>
    </Modal>
  );
}
