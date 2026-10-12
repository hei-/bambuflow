import { PauseCircleOutlined, PlayCircleOutlined, PlusOutlined, SyncOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Empty,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Spin,
  Table,
  Tag,
  message,
} from "antd";
import { useRef, useState } from "react";
import { ApiError, describe, newIdempotencyKey, request } from "../api/client";
import type {
  DeletionHold,
  DeletionHoldList,
  GrantApplication,
  GrantApplicationList,
  PauseSourceResult,
  Source,
  SourceGrant,
  SourceList,
  SyncNowResult,
  SyncRun,
  SyncStatus,
} from "../api/types";
import { ConfirmAction } from "../components/ConfirmAction";
import { ErrorAlert } from "../components/ErrorAlert";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtDuration, fmtTime } from "../format";
import { useLoad } from "../useLoad";

const typeLabel: Record<Source["type"], string> = {
  wiki: "Wiki",
  mail: "邮件",
  file: "文件",
  image: "图片",
  table: "表格",
  log: "日志",
  db: "数据库",
};

const classLabel: Record<Source["default_classification"], string> = {
  public: "公开",
  internal: "内部",
  confidential: "机密",
  restricted: "受限",
};

// 数据源（governance 详设 6.2，入库管道详设 14.4）：平台管理员注册，本部门的数据属主审批（受限级另需信息安全审批）、
// 授权给租户与撤销；租户负责人申请授权、属主审批申请；同步状态、暂停与恢复、立即同步、删除保护拦下的待删批次。能看到哪些数据源由 gateway 按角色决定
export function SourcesPage() {
  const { session } = useConsole();
  const list = useLoad((signal) => request<SourceList>(session, "/v1/admin/sources", { signal }), "sources");
  const [selected, setSelected] = useState<string>();
  const sources = list.data?.sources ?? [];
  const current = sources.find((s) => s.source_id === selected);
  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card title="数据源" extra={<RegisterButton onDone={list.reload} />}>
        {list.error !== undefined ? (
          <ErrorAlert error={list.error} onRetry={list.reload} />
        ) : (
          <Table<Source>
            rowKey="source_id"
            size="small"
            loading={list.loading}
            dataSource={sources}
            pagination={{ pageSize: 20, hideOnSinglePage: true }}
            locale={{ emptyText: "没有可见的数据源" }}
            rowSelection={{
              type: "radio",
              selectedRowKeys: selected ? [selected] : [],
              onChange: (keys) => setSelected(String(keys[0])),
            }}
            columns={[
              { title: "数据源", render: (_, s) => s.description || s.source_id },
              { title: "ID", dataIndex: "source_id" },
              { title: "类型", dataIndex: "type", render: (t: Source["type"]) => typeLabel[t] },
              { title: "属主部门", dataIndex: "owner_dept" },
              {
                title: "密级",
                dataIndex: "default_classification",
                render: (c: Source["default_classification"]) => classLabel[c],
              },
              { title: "状态", render: (_, s) => <SourceStatusTag source={s} /> },
            ]}
          />
        )}
      </Card>
      <ApplicationsCard sources={sources.filter((s) => s.status === "active")} />
      {current && <SourceDetail key={current.source_id} source={current} onChange={list.reload} />}
    </Space>
  );
}

// 待审批时写明还差哪一项：受限级要属主与信息安全各批一次
export function SourceStatusTag({ source: s }: { source: Source }) {
  if (s.status === "active") return <Tag color="green">已生效</Tag>;
  if (s.status === "revoked") return <Tag color="red">已撤销</Tag>;
  const missing = [
    !s.approved_by && "属主",
    s.default_classification === "restricted" && !s.security_approved_by && "信息安全",
  ].filter(Boolean);
  return <Tag color="orange">待{missing.join("、")}审批</Tag>;
}

interface RegisterForm {
  type: Source["type"];
  owner_dept: string;
  default_classification: Source["default_classification"];
  description: string;
  push_client?: string;
  permission_model: Source["permission_model"];
}

function RegisterButton({ onDone }: { onDone: () => void }) {
  const { session } = useConsole();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<RegisterForm>();
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const v = await form.validateFields();
    setBusy(true);
    try {
      const s = await request<Source>(session, "/v1/admin/sources", {
        body: {
          ...v,
          owner_dept: v.owner_dept.trim(),
          description: v.description.trim(),
          push_client: v.push_client?.trim() ?? "",
        },
        idempotencyKey: keyRef.current,
      });
      keyRef.current = newIdempotencyKey();
      void message.success(`已注册 ${s.source_id}，待数据属主审批`);
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
        注册数据源
      </Button>
      <Modal
        open={open}
        title="注册数据源"
        okText="注册"
        cancelText="取消"
        confirmLoading={busy}
        onOk={() => void submit()}
        onCancel={() => setOpen(false)}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{ type: "wiki", default_classification: "internal", permission_model: "document" }}
        >
          <Form.Item name="type" label="类型" rules={[{ required: true }]}>
            <Select options={Object.entries(typeLabel).map(([value, label]) => ({ value, label }))} />
          </Form.Item>
          <Form.Item name="owner_dept" label="数据属主部门" rules={[{ required: true, whitespace: true, max: 64 }]}>
            <Input placeholder="如 信贷部" />
          </Form.Item>
          <Form.Item
            name="default_classification"
            label="默认密级"
            extra="受限级数据源要本部门属主与信息安全各审批一次"
            rules={[{ required: true }]}
          >
            <Select options={Object.entries(classLabel).map(([value, label]) => ({ value, label }))} />
          </Form.Item>
          <Form.Item name="description" label="名称与用途" rules={[{ required: true, whitespace: true, max: 500 }]}>
            <Input.TextArea autoSize={{ minRows: 2 }} maxLength={500} showCount />
          </Form.Item>
          <Form.Item
            name="permission_model"
            label="权限模型"
            extra="分区级：按知识库或目录给部门、群组授权，文档只带分区，授权变化不改索引；邮件、有页面限制的 Wiki 等用文档级"
            rules={[{ required: true }]}
          >
            <Select
              options={[
                { value: "document", label: "文档级（每份文档带自己的 ACL）" },
                { value: "partition", label: "分区级（按知识库或目录授权）" },
              ]}
            />
          </Form.Item>
          <Form.Item
            name="push_client"
            label="推送客户端（可选）"
            extra="业务系统经推送入库 API 写这个数据源时用的服务账号客户端 ID，随数据源一起审批"
            rules={[{ pattern: /^[a-z][a-z0-9-]{1,62}$/, message: "小写字母开头，只含小写字母、数字和连字符" }]}
          >
            <Input placeholder="如 crm-sync" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}

function SourceDetail({ source, onChange }: { source: Source; onChange: () => void }) {
  return (
    <>
      <GovernanceCard source={source} onChange={onChange} />
      {source.status === "active" && <GrantsCard source={source.source_id} />}
      {source.status !== "pending" && <SyncDetail source={source.source_id} />}
    </>
  );
}

// 审批与撤销：审批依据（如内部工单号）必填；审批要求刚做过加强认证
function GovernanceCard({ source: s, onChange }: { source: Source; onChange: () => void }) {
  const { session } = useConsole();
  const base = `/v1/admin/sources/${encodeURIComponent(s.source_id)}`;
  const approve = (capacity: "data_owner" | "infosec") => async (basis: string, key: string) => {
    const r = await request<Source>(session, `${base}/approve`, { body: { basis, capacity }, idempotencyKey: key });
    return r.status === "active" ? "已审批，数据源生效" : "已审批，还待另一项审批";
  };
  return (
    <Card
      title={s.description || s.source_id}
      extra={
        <Space>
          {s.status === "pending" && !s.approved_by && (
            <ReasonAction
              label="属主审批"
              title="以数据属主身份审批"
              fieldLabel="审批依据"
              onSubmit={approve("data_owner")}
              onDone={onChange}
            />
          )}
          {s.status === "pending" && s.default_classification === "restricted" && !s.security_approved_by && (
            <ReasonAction
              label="信息安全审批"
              title="以信息安全身份审批受限级数据源"
              fieldLabel="审批依据"
              onSubmit={approve("infosec")}
              onDone={onChange}
            />
          )}
          {s.status !== "revoked" && (
            <ReasonAction
              label="撤销"
              title="撤销数据源：在线立即不可见，全部文档写删除标记，不能恢复"
              danger
              onSubmit={async (reason, key) => {
                await request<Source>(session, `${base}/revoke`, { body: { reason }, idempotencyKey: key });
                return "已撤销";
              }}
              onDone={onChange}
            />
          )}
        </Space>
      }
    >
      <Descriptions size="small" column={2} bordered>
        <Descriptions.Item label="状态">
          <SourceStatusTag source={s} />
        </Descriptions.Item>
        <Descriptions.Item label="注册">
          {s.created_by}，{fmtTime(s.created_at)}
        </Descriptions.Item>
        <Descriptions.Item label="属主审批">
          {s.approved_by ? `${s.approved_by}，${fmtTime(s.approved_at)}，${s.approval_basis ?? ""}` : "—"}
        </Descriptions.Item>
        <Descriptions.Item label="信息安全审批">
          {s.security_approved_by
            ? `${s.security_approved_by}，${fmtTime(s.security_approved_at)}，${s.security_approval_basis ?? ""}`
            : s.default_classification === "restricted"
              ? "—"
              : "不需要"}
        </Descriptions.Item>
        <Descriptions.Item label="权限模型">
          {s.permission_model === "partition" ? "分区级" : "文档级"}
        </Descriptions.Item>
        <Descriptions.Item label="推送入库">
          {s.push_client ? `接受推送，客户端 ${s.push_client}` : "不接受推送"}
        </Descriptions.Item>
        {s.revoked_by && (
          <Descriptions.Item label="撤销" span={2}>
            {s.revoked_by}，{fmtTime(s.revoked_at)}，{s.revoke_reason}
          </Descriptions.Item>
        )}
      </Descriptions>
    </Card>
  );
}

const applicationStatus: Record<GrantApplication["status"], { label: string; color?: string }> = {
  pending: { label: "待审批", color: "orange" },
  approved: { label: "已授权", color: "green" },
  rejected: { label: "已驳回", color: "red" },
  withdrawn: { label: "已撤回" },
  expired: { label: "已过期" },
};

// 授权申请（governance 详设 6.2）：租户负责人为自己负责的租户申请，数据源所属部门的数据属主审批或驳回，申请人可以撤回。
// 两种身份都没有的用户（403）不显示这张卡片；能看到哪些申请、能否审批由 governance 判断
function ApplicationsCard({ sources }: { sources: Source[] }) {
  const { session, config } = useConsole();
  const base = "/v1/admin/grant-applications";
  const list = useLoad((signal) => request<GrantApplicationList>(session, base, { signal }), "grant-applications");
  const [source, setSource] = useState<string>();
  const [tenant, setTenant] = useState<string>();
  if (list.error instanceof ApiError && list.error.status === 403) return null;
  const act = (a: GrantApplication, verb: "approve" | "reject") => async (text: string, key: string) => {
    const body = verb === "approve" ? { basis: text } : { reason: text };
    await request(session, `${base}/${encodeURIComponent(a.application_id)}/${verb}`, { body, idempotencyKey: key });
    return verb === "approve" ? "已审批，1 分钟内该租户可检索这个数据源" : "已处理";
  };
  return (
    <Card
      title="授权申请"
      extra={
        <Space.Compact>
          <Select
            style={{ width: 200 }}
            placeholder="数据源"
            value={source}
            onChange={setSource}
            options={sources.map((s) => ({ value: s.source_id, label: s.description || s.source_id }))}
          />
          <Select
            style={{ width: 140 }}
            placeholder="租户"
            value={tenant}
            onChange={setTenant}
            options={config.tenants.map((t) => ({ value: t.id, label: t.name }))}
          />
          <ReasonAction
            label="申请授权"
            title="为租户申请这个数据源（由数据属主审批）"
            fieldLabel="用途"
            disabled={!source || !tenant}
            onSubmit={async (reason, key) => {
              await request<GrantApplication>(session, base, {
                body: { source_id: source, tenant, reason },
                idempotencyKey: key,
              });
              setSource(undefined);
              setTenant(undefined);
              return "已提交，待数据属主审批";
            }}
            onDone={list.reload}
          />
        </Space.Compact>
      }
    >
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <Table<GrantApplication>
          rowKey="application_id"
          size="small"
          loading={list.loading}
          dataSource={list.data?.applications ?? []}
          pagination={{ pageSize: 10, hideOnSinglePage: true }}
          locale={{ emptyText: "没有授权申请" }}
          columns={[
            { title: "数据源", dataIndex: "source_id" },
            { title: "租户", dataIndex: "tenant" },
            { title: "用途", dataIndex: "reason" },
            { title: "申请人", render: (_, a) => `${a.applied_by}，${fmtTime(a.applied_at)}` },
            {
              title: "状态",
              render: (_, a) => (
                <Tag color={applicationStatus[a.status].color}>{applicationStatus[a.status].label}</Tag>
              ),
            },
            {
              title: "处理",
              render: (_, a) =>
                a.status === "pending" ? (
                  <Space>
                    <ReasonAction
                      label="审批"
                      title="审批授权申请"
                      fieldLabel="审批依据"
                      onSubmit={act(a, "approve")}
                      onDone={list.reload}
                    />
                    <ReasonAction
                      label="驳回或撤回"
                      title="驳回（属主）或撤回（申请人）"
                      danger
                      onSubmit={act(a, "reject")}
                      onDone={list.reload}
                    />
                  </Space>
                ) : (
                  a.decided_by && `${a.decided_by}：${a.decision_note ?? ""}`
                ),
            },
          ]}
        />
      )}
    </Card>
  );
}

// 租户授权（本部门的数据属主）：授权与取消都在 1 分钟内对检索生效
function GrantsCard({ source }: { source: string }) {
  const { session, config } = useConsole();
  const base = `/v1/admin/sources/${encodeURIComponent(source)}/grants`;
  const grants = useLoad((signal) => request<{ grants: SourceGrant[] }>(session, base, { signal }), `grants/${source}`);
  const [tenant, setTenant] = useState<string>();
  const granted = new Set((grants.data?.grants ?? []).map((g) => g.tenant));
  return (
    <Card
      title="租户授权"
      extra={
        <Space.Compact>
          <Select
            style={{ width: 160 }}
            placeholder="选择租户"
            value={tenant}
            onChange={setTenant}
            options={config.tenants.filter((t) => !granted.has(t.id)).map((t) => ({ value: t.id, label: t.name }))}
          />
          <ConfirmAction
            label="授权"
            title={`把这个数据源授权给 ${tenant ?? ""}？`}
            disabled={!tenant}
            onSubmit={async (key) => {
              await request<SourceGrant>(session, base, { body: { tenant }, idempotencyKey: key });
              setTenant(undefined);
              return "已授权";
            }}
            onDone={grants.reload}
          />
        </Space.Compact>
      }
    >
      {grants.error !== undefined ? (
        <ErrorAlert error={grants.error} onRetry={grants.reload} />
      ) : (
        <Table<SourceGrant>
          rowKey="tenant"
          size="small"
          loading={grants.loading}
          dataSource={grants.data?.grants ?? []}
          pagination={false}
          locale={{ emptyText: "还没有授权给任何租户" }}
          columns={[
            { title: "租户", dataIndex: "tenant" },
            { title: "授权人", dataIndex: "granted_by" },
            { title: "时间", dataIndex: "granted_at", render: fmtTime },
            {
              title: "",
              render: (_, g) => (
                <ConfirmAction
                  label="取消授权"
                  title={`取消对 ${g.tenant} 的授权？1 分钟内该租户就检索不到这个数据源`}
                  danger
                  onSubmit={async (key) => {
                    await request(session, base, {
                      method: "DELETE",
                      query: { tenant: g.tenant },
                      idempotencyKey: key,
                    });
                    return "已取消授权";
                  }}
                  onDone={grants.reload}
                />
              ),
            },
          ]}
        />
      )}
    </Card>
  );
}

// 同步运维（入库管道详设 14.4）：同步状态与对账结果、暂停与恢复、立即同步、删除保护拦下的待删批次
function SyncDetail({ source }: { source: string }) {
  const { session } = useConsole();
  const base = `/v1/admin/sources/${encodeURIComponent(source)}`;
  const status = useLoad((signal) => request<SyncStatus>(session, `${base}/sync-status`, { signal }), source);
  const holds = useLoad(
    (signal) => request<DeletionHoldList>(session, `${base}/pending-deletions`, { signal }),
    source,
  );
  const reloadAll = () => {
    status.reload();
    holds.reload();
  };
  const notSynced = status.error instanceof ApiError && status.error.status === 404;
  const st = status.data;
  return (
    <>
      <Card
        title="同步状态"
        extra={
          <Space>
            {st?.pause ? (
              <ReasonAction
                label="恢复"
                title="恢复同步"
                icon={<PlayCircleOutlined />}
                onSubmit={async (_, key) => {
                  await request(session, `${base}/resume`, { method: "POST", idempotencyKey: key });
                }}
                onDone={reloadAll}
              />
            ) : (
              <ReasonAction
                label="暂停"
                title="暂停同步（暂停期间该源的撤权与删除也不再同步）"
                danger
                icon={<PauseCircleOutlined />}
                onSubmit={async (reason, key) => {
                  await request<PauseSourceResult>(session, `${base}/pause`, { body: { reason }, idempotencyKey: key });
                }}
                onDone={reloadAll}
              />
            )}
            <ReasonAction
              label="立即同步"
              title="立即拉起一轮同步"
              icon={<SyncOutlined />}
              disabled={!!st?.pause || !!st?.revoked_at}
              onSubmit={async (reason, key) => {
                const r = await request<SyncNowResult>(session, `${base}/sync`, {
                  body: { reason },
                  idempotencyKey: key,
                });
                return `已拉起同步任务 ${r.job}`;
              }}
              onDone={reloadAll}
            />
          </Space>
        }
      >
        {status.loading && <Spin />}
        {notSynced && <Empty description="这个数据源还没有同步记录" />}
        {status.error !== undefined && !notSynced && <ErrorAlert error={status.error} onRetry={status.reload} />}
        {st && (
          <Space direction="vertical" style={{ width: "100%" }}>
            {st.revoked_at && (
              <Alert
                type="error"
                showIcon
                message={`已撤销（${fmtTime(st.revoked_at)}）`}
                description="全部文档已写删除标记，观察期后物理删除；之后的同步都跳过，不能恢复。"
              />
            )}
            {st.pause && (
              <Alert
                type="warning"
                showIcon
                message={`已暂停：${st.pause.reason}`}
                description={`${st.pause.paused_by} 于 ${fmtTime(st.pause.paused_at)} 暂停`}
              />
            )}
            <Descriptions size="small" column={2} bordered>
              <Descriptions.Item label="新鲜度">{fmtDuration(st.freshness_seconds)}</Descriptions.Item>
              <Descriptions.Item label="最近一轮">{runSummary(st.last_run)}</Descriptions.Item>
              <Descriptions.Item label="清单轮询">
                {st.listing_interval_seconds
                  ? `每 ${Math.round(st.listing_interval_seconds / 60)} 分钟（按源系统的响应自动调整），下一轮 ${fmtTime(st.next_round_at)}`
                  : "按定时任务同步"}
              </Descriptions.Item>
            </Descriptions>
            <RunView title="最近一轮同步" run={st.last_run} />
            <RunView title="最近一次对账" run={st.last_reconcile} />
          </Space>
        )}
      </Card>
      <Card title="删除保护拦下的待删批次">
        {holds.error !== undefined && !(holds.error instanceof ApiError && holds.error.status === 404) ? (
          <ErrorAlert error={holds.error} onRetry={holds.reload} />
        ) : (
          <HoldsTable source={source} holds={holds.data?.holds ?? []} loading={holds.loading} onChange={reloadAll} />
        )}
      </Card>
    </>
  );
}

function runSummary(run?: SyncRun) {
  if (!run) return "—";
  const color = run.result === "succeeded" ? "success" : run.result === "failed" ? "error" : "warning";
  const text = { succeeded: "成功", failed: "失败", skipped_paused: "暂停跳过", skipped_revoked: "已撤销，跳过" }[
    run.result
  ];
  return (
    <Space>
      <Tag color={color}>{text}</Tag>
      {fmtTime(run.finished_at)}
    </Space>
  );
}

function RunView({ title, run }: { title: string; run?: SyncRun }) {
  if (!run) return null;
  const r = run.reconciliation;
  return (
    <Descriptions size="small" title={title} column={3} bordered>
      <Descriptions.Item label="命令">{run.command}</Descriptions.Item>
      <Descriptions.Item label="结果">{runSummary(run)}</Descriptions.Item>
      <Descriptions.Item label="文档数">{run.documents}</Descriptions.Item>
      <Descriptions.Item label="内容变更">{run.upserts}</Descriptions.Item>
      <Descriptions.Item label="权限变更">{run.acl_only}</Descriptions.Item>
      <Descriptions.Item label="删除">
        {run.deletes}
        {run.deletes_paused && <Tag color="warning">删除保护触发（{run.deletes_paused_reason}）</Tag>}
      </Descriptions.Item>
      {run.error && (
        <Descriptions.Item label="错误" span={3}>
          {run.error}
        </Descriptions.Item>
      )}
      {!!run.skipped && (
        <Descriptions.Item label={`跳过 ${run.skipped} 份（读不出，本轮不推断删除）`} span={3}>
          {(run.skipped_items ?? []).join("；")}
        </Descriptions.Item>
      )}
      {r && (
        <>
          <Descriptions.Item label="已入库">{r.indexed}</Descriptions.Item>
          <Descriptions.Item label="漏同步（补发）">{r.missing}</Descriptions.Item>
          <Descriptions.Item label="内容不一致（补发）">{r.content_mismatch}</Descriptions.Item>
          <Descriptions.Item label="权限不一致（补发）">{r.acl_mismatch}</Descriptions.Item>
          <Descriptions.Item label="源端已删（补删）">{r.orphaned}</Descriptions.Item>
          <Descriptions.Item label="已删除、源端仍在">{r.tombstoned_in_source}</Descriptions.Item>
        </>
      )}
    </Descriptions>
  );
}

const holdStatus: Record<DeletionHold["status"], string> = {
  pending: "待确认",
  awaiting_second: "待第二位属主复核",
  confirmed: "已确认，下一轮执行",
  applied: "已执行",
  discarded: "已丢弃",
  superseded: "已被新批次取代",
};

function HoldsTable({
  source,
  holds,
  loading,
  onChange,
}: {
  source: string;
  holds: DeletionHold[];
  loading: boolean;
  onChange: () => void;
}) {
  const { session } = useConsole();
  const act = (h: DeletionHold, action: "confirm" | "discard") => async (reason: string, key: string) => {
    await request(session, `/v1/admin/sources/${encodeURIComponent(source)}/pending-deletions/${h.hold_id}/${action}`, {
      body: { reason },
      idempotencyKey: key,
    });
  };
  return (
    <Table
      rowKey="hold_id"
      size="small"
      loading={loading}
      dataSource={holds}
      pagination={false}
      locale={{ emptyText: "没有被拦下的删除" }}
      columns={[
        { title: "批次", dataIndex: "hold_id" },
        { title: "发现时间", dataIndex: "detected_at", render: fmtTime },
        { title: "待删文档", render: (_, h) => `${h.doc_ids.length} / 已同步 ${h.synced_docs}` },
        { title: "状态", dataIndex: "status", render: (s: DeletionHold["status"]) => <Tag>{holdStatus[s]}</Tag> },
        { title: "确认人", render: (_, h) => [h.confirmed_by, h.approved_by].filter(Boolean).join("、") || "—" },
        {
          title: "操作",
          render: (_, h) =>
            h.status === "pending" || h.status === "awaiting_second" ? (
              <Space>
                <ReasonAction
                  label="确认删除"
                  title="确认删除（需两位不同的数据属主）"
                  danger
                  onSubmit={act(h, "confirm")}
                  onDone={onChange}
                />
                <ReasonAction label="丢弃" title="丢弃这批删除" onSubmit={act(h, "discard")} onDone={onChange} />
              </Space>
            ) : null,
        },
      ]}
    />
  );
}
