import { PlusOutlined } from "@ant-design/icons";
import { Alert, Button, Card, Form, Input, Modal, Select, Space, Table, Tag, Typography, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type {
  ChangeRequest,
  ChangeRequestList,
  ConfigVersion,
  ConfigVersionList,
  Release,
  ReleaseList,
  Tenant,
  TenantList,
} from "../api/types";
import { ConfirmAction } from "../components/ConfirmAction";
import { ErrorAlert } from "../components/ErrorAlert";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

// 租户配置（governance 详设 3、4，平台管理员）：看生效版本与历史，新建租户或基于生效版本提交变更单；
// 变更单与 CI 发起的发布由作者以外的平台管理员审批后生效（审批要求刚做过加强认证），生效中的发布可以一人回退
export function TenantsPage() {
  const { session } = useConsole();
  const list = useLoad((signal) => request<TenantList>(session, "/v1/admin/tenants", { signal }), "tenants");
  const [selected, setSelected] = useState<string>();
  const [created, setCreated] = useState<ChangeRequest>();
  // 提交、审批或回退后，租户列表、变更单与发布一起刷新
  const [tick, setTick] = useState(0);
  const changed = () => {
    list.reload();
    setTick((t) => t + 1);
  };
  const onCreated = (c: ChangeRequest) => {
    setCreated(c);
    changed();
  };
  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card title="租户配置" extra={<CreateTenantButton onCreated={onCreated} />}>
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <Table<Tenant>
            rowKey="tenant"
            size="small"
            loading={list.loading}
            dataSource={list.data?.tenants ?? []}
            pagination={false}
            locale={{ emptyText: "还没有租户" }}
            rowSelection={{
              type: "radio",
              selectedRowKeys: selected ? [selected] : [],
              onChange: (keys) => setSelected(String(keys[0])),
            }}
            columns={[
              { title: "租户", dataIndex: "tenant" },
              {
                title: "接入阶段",
                dataIndex: "rollout",
                render: (r: string) => (
                  <Tag color={r === "general" ? "green" : "orange"}>{r === "general" ? "全量" : "试点"}</Tag>
                ),
              },
              { title: "生效版本", render: (_, t) => `#${t.active.number}（${t.active.version_id}）` },
              { title: "提交人", render: (_, t) => t.active.created_by },
              { title: "时间", render: (_, t) => fmtTime(t.active.created_at) },
              { title: "", render: (_, t) => <ChangeButton tenant={t} onCreated={onCreated} /> },
            ]}
          />
        )}
      </Card>
      {created && (
        <Alert
          type="info"
          showIcon
          closable
          onClose={() => setCreated(undefined)}
          message={`已提交变更单：${created.chg_id}`}
          description="请另一位平台管理员在下方“变更单”里审批。"
        />
      )}
      <ChangeRequestsCard reloadKey={tick} onDecided={changed} />
      <ReleasesCard reloadKey={tick} onChange={changed} />
      {selected && <Versions key={`${selected}/${tick}`} tenant={selected} />}
    </Space>
  );
}

function Versions({ tenant }: { tenant: string }) {
  const { session } = useConsole();
  const versions = useLoad(
    (signal) =>
      request<ConfigVersionList>(session, `/v1/admin/tenants/${encodeURIComponent(tenant)}/config-versions`, {
        signal,
      }),
    tenant,
  );
  return (
    <Card title={`${tenant} 的配置版本`}>
      {versions.error !== undefined ? (
        <ErrorAlert error={versions.error} onRetry={versions.reload} />
      ) : (
        <Table<ConfigVersion>
          rowKey="version_id"
          size="small"
          loading={versions.loading}
          dataSource={versions.data?.versions ?? []}
          pagination={false}
          expandable={{
            expandedRowRender: (v) => <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>{v.content}</pre>,
          }}
          columns={[
            { title: "版本", render: (_, v) => `#${v.number}` },
            { title: "版本 ID", dataIndex: "version_id" },
            { title: "来源", dataIndex: "source_ref" },
            { title: "提交人", dataIndex: "created_by" },
            { title: "时间", dataIndex: "created_at", render: fmtTime },
            { title: "", dataIndex: "active", render: (a: boolean) => (a ? <Tag color="green">生效中</Tag> : null) },
          ]}
        />
      )}
    </Card>
  );
}

// 基于生效版本提交变更单：内容是完整的 tenant.yaml，切到全量时附评测门禁 ID
function ChangeButton({ tenant, onCreated }: { tenant: Tenant; onCreated: (c: ChangeRequest) => void }) {
  const { session } = useConsole();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<{ content: string; reason: string; gate_id?: string }>();
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      const c = await request<ChangeRequest>(session, "/v1/admin/change-requests", {
        body: {
          object: "tenant",
          scope: tenant.tenant,
          base_version_id: tenant.active.version_id,
          content: v.content,
          reason: v.reason.trim(),
          gate_id: v.gate_id?.trim() || undefined,
        },
        idempotencyKey: keyRef.current,
      });
      keyRef.current = newIdempotencyKey();
      setOpen(false);
      onCreated(c);
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      <Button
        size="small"
        onClick={() => {
          form.setFieldsValue({ content: tenant.active.content, reason: "", gate_id: "" });
          setOpen(true);
        }}
      >
        提交变更
      </Button>
      <Modal
        open={open}
        width={720}
        title={`修改 ${tenant.tenant} 的配置（基于版本 #${tenant.active.number}）`}
        okText="提交变更单"
        cancelText="取消"
        confirmLoading={busy}
        onOk={() => void submit()}
        onCancel={() => setOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="content" label="tenant.yaml" rules={[{ required: true, whitespace: true }]}>
            <Input.TextArea autoSize={{ minRows: 8, maxRows: 24 }} style={{ fontFamily: "monospace" }} />
          </Form.Item>
          <Form.Item name="reason" label="原因" rules={[{ required: true, whitespace: true, max: 500 }]}>
            <Input.TextArea autoSize={{ minRows: 2 }} maxLength={500} showCount />
          </Form.Item>
          <Form.Item name="gate_id" label="评测门禁 ID（从试点切到全量时必填）">
            <Input placeholder="gate_ 开头" allowClear />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}

const chgStatus: Record<string, { label: string; color: string }> = {
  open: { label: "待审批", color: "orange" },
  approved: { label: "已生效", color: "green" },
  rejected: { label: "已驳回", color: "default" },
  stale: { label: "已作废", color: "default" },
  merge_request: { label: "合并请求中", color: "blue" },
  expired: { label: "超时关闭", color: "default" },
};

// 变更单：按状态列出，待审批的由作者以外的平台管理员审批或驳回（审批要求刚做过加强认证）
function ChangeRequestsCard({ reloadKey, onDecided }: { reloadKey: number; onDecided: () => void }) {
  const { session } = useConsole();
  const [status, setStatus] = useState("open");
  const list = useLoad(
    (signal) => request<ChangeRequestList>(session, "/v1/admin/change-requests", { query: { status }, signal }),
    `${status}/${reloadKey}`,
  );
  const done = () => {
    list.reload();
    onDecided();
  };
  return (
    <Card
      title="变更单"
      extra={
        <Select
          style={{ width: 120 }}
          value={status}
          onChange={setStatus}
          options={Object.entries(chgStatus).map(([value, s]) => ({ value, label: s.label }))}
        />
      }
    >
      <Typography.Paragraph type="secondary">
        作者不能审批自己的变更单；审批时生效版本已经变了，变更单作废，需要基于最新版本重新提交。
      </Typography.Paragraph>
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <Table<ChangeRequest>
          rowKey="chg_id"
          size="small"
          loading={list.loading}
          dataSource={list.data?.change_requests ?? []}
          pagination={{ pageSize: 20, hideOnSinglePage: true }}
          locale={{ emptyText: "没有这类变更单" }}
          expandable={{
            expandedRowRender: (c) => <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>{c.version?.content}</pre>,
          }}
          columns={[
            { title: "变更单", dataIndex: "chg_id" },
            { title: "租户", dataIndex: "scope" },
            { title: "原因", dataIndex: "reason" },
            { title: "门禁", dataIndex: "gate_id", render: (g?: string) => g || "—" },
            { title: "作者", dataIndex: "author" },
            { title: "提交时间", dataIndex: "created_at", render: fmtTime },
            {
              title: "状态",
              dataIndex: "status",
              render: (st: string) => <Tag color={chgStatus[st]?.color}>{chgStatus[st]?.label ?? st}</Tag>,
            },
            {
              title: "",
              render: (_, c) =>
                c.status === "open" ? (
                  <Space>
                    <ConfirmAction
                      label="审批通过"
                      title={`审批 ${c.scope} 的变更单？通过后立即生效`}
                      onSubmit={async (key) => {
                        const r = await request<ChangeRequest>(
                          session,
                          `/v1/admin/change-requests/${c.chg_id}/approve`,
                          {
                            method: "POST",
                            idempotencyKey: key,
                          },
                        );
                        return r.release_id ? `已生效，发布 ${r.release_id}` : `变更单状态：${r.status}`;
                      }}
                      onDone={done}
                    />
                    <ReasonAction
                      label="驳回"
                      title={`驳回 ${c.chg_id}`}
                      danger
                      onSubmit={async (reason, key) => {
                        await request<ChangeRequest>(session, `/v1/admin/change-requests/${c.chg_id}/reject`, {
                          body: { reason },
                          idempotencyKey: key,
                        });
                        return "已驳回";
                      }}
                      onDone={done}
                    />
                  </Space>
                ) : (
                  c.decided_by && `${c.decided_by}，${fmtTime(c.decided_at)}`
                ),
            },
          ]}
        />
      )}
    </Card>
  );
}

const releaseStatus: Record<string, { label: string; color: string }> = {
  gating: { label: "门禁中", color: "processing" },
  gate_failed: { label: "门禁未通过", color: "red" },
  pending_approval: { label: "待审批", color: "orange" },
  pending_second_approval: { label: "待第二位审批", color: "orange" },
  rolling_out: { label: "灰度中", color: "blue" },
  active: { label: "生效中", color: "green" },
  superseded: { label: "已被取代", color: "default" },
  rolled_back: { label: "已回退", color: "red" },
  rejected: { label: "已驳回", color: "default" },
  stale: { label: "已作废", color: "default" },
  expired: { label: "超时关闭", color: "default" },
};

const releaseSource: Record<string, string> = {
  ci: "CI",
  change_request: "变更单",
  rollback: "回退",
  bootstrap: "初始导入",
};

// 发布：CI 在 Git 合并后发起；影响回答的对象与策略包先由 governance 用候选版本跑评测门禁，通过后平台管理员审批；
// 生效中的发布可以一人回退到发布前的版本
function ReleasesCard({ reloadKey, onChange }: { reloadKey: number; onChange: () => void }) {
  const { session } = useConsole();
  const [status, setStatus] = useState<string>("");
  const list = useLoad(
    (signal) => request<ReleaseList>(session, "/v1/admin/releases", { query: { status: status || undefined }, signal }),
    `${status}/${reloadKey}`,
  );
  const done = () => {
    list.reload();
    onChange();
  };
  return (
    <Card
      title="发布"
      extra={
        <Select
          style={{ width: 120 }}
          value={status}
          onChange={setStatus}
          options={[
            { value: "", label: "全部" },
            ...Object.entries(releaseStatus).map(([value, s]) => ({ value, label: s.label })),
          ]}
        />
      }
    >
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <Table<Release>
          rowKey="release_id"
          size="small"
          loading={list.loading}
          dataSource={list.data?.releases ?? []}
          pagination={{ pageSize: 20, hideOnSinglePage: true }}
          locale={{ emptyText: "没有这类发布" }}
          columns={[
            { title: "发布", dataIndex: "release_id" },
            { title: "租户", dataIndex: "scope" },
            { title: "版本", render: (_, r) => (r.version ? `#${r.version.number}` : "—") },
            { title: "来源", render: (_, r) => `${releaseSource[r.source] ?? r.source}（${r.source_ref}）` },
            { title: "原因", dataIndex: "reason" },
            { title: "发起人", dataIndex: "requested_by" },
            { title: "时间", dataIndex: "requested_at", render: fmtTime },
            {
              title: "状态",
              dataIndex: "status",
              render: (st: string, r) => (
                <Space size={4} wrap>
                  <Tag color={releaseStatus[st]?.color}>{releaseStatus[st]?.label ?? st}</Tag>
                  {/* 门禁报告在“评测门禁”页按编号查看 */}
                  {(st === "gating" || st === "gate_failed") && (
                    <Typography.Text type="secondary">
                      {r.gate_id ? `门禁 ${r.gate_id}` : "等待 evaluation 接收门禁"}
                    </Typography.Text>
                  )}
                  {r.rollout && (
                    <Typography.Text type="secondary">
                      {r.rollout.tenant} 的 {r.rollout.percent}% 用户
                    </Typography.Text>
                  )}
                  {/* 观察 30 分钟：超阈值自动终止，期满没有超阈值自动全量（governance 详设 4.1） */}
                  {r.rollout && (
                    <Typography.Text type="secondary">
                      {r.rollout.observation
                        ? `观察（${fmtTime(r.rollout.observed_at ?? "")}）：${r.rollout.observation}`
                        : "等待第一次观察"}
                    </Typography.Text>
                  )}
                </Space>
              ),
            },
            {
              title: "",
              render: (_, r) =>
                r.status === "pending_approval" || r.status === "pending_second_approval" ? (
                  <ConfirmAction
                    label="审批通过"
                    title={
                      r.object === "policy" && r.status === "pending_approval"
                        ? `审批 ${r.scope} 的策略层？还要另一位审批人（至少一位是信息安全或数据属主）`
                        : `审批 ${r.scope} 的发布？通过后立即生效`
                    }
                    onSubmit={async (key) => {
                      const x = await request<Release>(session, `/v1/admin/releases/${r.release_id}/approve`, {
                        method: "POST",
                        idempotencyKey: key,
                      });
                      return `发布状态：${releaseStatus[x.status]?.label ?? x.status}`;
                    }}
                    onDone={done}
                  />
                ) : r.status === "rolling_out" ? (
                  <Space>
                    <ConfirmAction
                      label="全量"
                      title={`${r.object} ${r.scope} 的灰度全量生效？所有租户与用户都用这个版本`}
                      onSubmit={async (key) => {
                        const x = await request<Release>(session, `/v1/admin/releases/${r.release_id}/promote`, {
                          method: "POST",
                          idempotencyKey: key,
                        });
                        return `发布状态：${releaseStatus[x.status]?.label ?? x.status}`;
                      }}
                      onDone={done}
                    />
                    <ReasonAction
                      label="终止灰度"
                      title={`终止 ${r.release_id} 的灰度，候选版本不再生效`}
                      danger
                      onSubmit={async (reason, key) => {
                        await request<Release>(session, `/v1/admin/releases/${r.release_id}/abort`, {
                          body: { reason },
                          idempotencyKey: key,
                        });
                        return "已终止灰度";
                      }}
                      onDone={done}
                    />
                  </Space>
                ) : r.status === "active" && r.from_version_id ? (
                  <ReasonAction
                    label="回退"
                    title={`回退 ${r.release_id}，切回发布之前的生效版本`}
                    danger
                    onSubmit={async (reason, key) => {
                      const x = await request<Release>(session, `/v1/admin/releases/${r.release_id}/rollback`, {
                        body: { reason },
                        idempotencyKey: key,
                      });
                      return `已回退，新的发布 ${x.release_id}`;
                    }}
                    onDone={done}
                  />
                ) : null,
            },
          ]}
        />
      )}
    </Card>
  );
}

const tenantPattern = /^[a-z][a-z0-9-]*$/;

// 新建租户：生成一张没有基础版本的变更单，另一位平台管理员审批后生效（接入手册 docs/onboarding.md）
function CreateTenantButton({ onCreated }: { onCreated: (c: ChangeRequest) => void }) {
  const { session } = useConsole();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<{ tenant: string; content: string; reason: string }>();
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      const c = await request<ChangeRequest>(session, "/v1/admin/tenants", {
        body: { tenant: v.tenant.trim(), content: v.content, reason: v.reason.trim() },
        idempotencyKey: keyRef.current,
      });
      keyRef.current = newIdempotencyKey();
      setOpen(false);
      form.resetFields();
      onCreated(c);
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      <Button size="small" icon={<PlusOutlined />} onClick={() => setOpen(true)}>
        新建租户
      </Button>
      <Modal
        open={open}
        width={720}
        title="新建租户（新租户先按试点接入）"
        okText="提交变更单"
        cancelText="取消"
        confirmLoading={busy}
        onOk={() => void submit()}
        onCancel={() => setOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="tenant"
            label="租户名"
            rules={[{ required: true, pattern: tenantPattern, message: "小写字母开头，只含小写字母、数字和连字符" }]}
          >
            <Input placeholder="如 compliance" />
          </Form.Item>
          <Form.Item
            name="content"
            label="tenant.yaml"
            rules={[{ required: true, whitespace: true }]}
            extra="字段与示例见 docs/onboarding.md；提交时按与 gateway 加载相同的规则校验"
          >
            <Input.TextArea autoSize={{ minRows: 8, maxRows: 24 }} style={{ fontFamily: "monospace" }} />
          </Form.Item>
          <Form.Item name="reason" label="原因" rules={[{ required: true, whitespace: true, max: 500 }]}>
            <Input.TextArea autoSize={{ minRows: 2 }} maxLength={500} showCount />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
