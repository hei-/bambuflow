import { PlusOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Modal,
  Progress,
  Select,
  Space,
  Spin,
  Table,
  Tag,
  message,
} from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type { ArmSummary, Experiment, ExperimentAnalysis, ExperimentList, ExperimentRequest } from "../api/types";
import { ConfirmAction } from "../components/ConfirmAction";
import { ErrorAlert } from "../components/ErrorAlert";
import { Pager, usePages } from "../components/Pager";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const metricLabel: Record<Experiment["primary_metric"], string> = {
  useful_rate: "“有用”反馈率",
  insufficient_evidence_rate: "证据不足比例",
};

// 流量只能按 5% → 20% → 50% 推进（governance 详设 7.2）
const nextTraffic: Record<number, 20 | 50 | undefined> = { 5: 20, 20: 50 };

const configVersion = /^cfv_[0-9a-f]{24}$/;

// 实验组替换了什么：阈值与配置版本（pipeline、routing、回答模板，都已单独过评测门禁）
function overridesText(o: Experiment["overrides"]): string {
  const parts: string[] = [];
  if (o.support_threshold !== undefined) parts.push(`支持度门槛 ${o.support_threshold}`);
  if (o.min_coverage !== undefined) parts.push(`覆盖率门槛 ${o.min_coverage}`);
  for (const v of o.config_versions ?? []) parts.push(`配置 ${v}`);
  return parts.join("；");
}

// A/B 实验（技术设计文档 10.5，governance 详设 7.2，evaluation 详设 7.2）：平台管理员立项、推进流量与停止；
// 展开一行看 A/B 分析，达到样本量前只显示进度，护栏触发时 evaluation 自动停止实验
export function ExperimentsPage() {
  const { session } = useConsole();
  const pages = usePages();
  const list = useLoad(
    (signal) =>
      request<ExperimentList>(session, "/v1/admin/experiments", {
        query: { page_size: 50, page_token: pages.token },
        signal,
      }),
    pages.token ?? "",
  );
  return (
    <Card title="A/B 实验" extra={<CreateButton onDone={list.reload} />}>
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <>
          <Table<Experiment>
            rowKey="exp_id"
            size="small"
            loading={list.loading}
            dataSource={list.data?.experiments ?? []}
            pagination={false}
            locale={{ emptyText: "还没有实验" }}
            expandable={{ expandedRowRender: (x) => <AnalysisView expId={x.exp_id} /> }}
            columns={[
              { title: "实验", dataIndex: "exp_id" },
              { title: "租户", dataIndex: "tenant" },
              { title: "假设", dataIndex: "hypothesis" },
              {
                title: "主要指标",
                dataIndex: "primary_metric",
                render: (m: Experiment["primary_metric"]) => metricLabel[m],
              },
              { title: "实验组替换", render: (_, x) => overridesText(x.overrides) },
              { title: "流量", dataIndex: "traffic_percent", render: (n: number) => `${n}%` },
              { title: "每组样本量", dataIndex: "sample_size_per_arm" },
              {
                title: "状态",
                render: (_, x) =>
                  x.status === "running" ? (
                    <Tag color="green">运行中</Tag>
                  ) : (
                    <Tag>
                      已停止（{x.stopped_by}：{x.stop_reason}）
                    </Tag>
                  ),
              },
              { title: "立项", render: (_, x) => `${x.created_by}，${fmtTime(x.created_at)}` },
              {
                title: "",
                render: (_, x) => (x.status === "running" ? <RowActions x={x} onDone={list.reload} /> : null),
              },
            ]}
          />
          <Pager pages={pages} nextToken={list.data?.next_page_token} />
        </>
      )}
    </Card>
  );
}

function RowActions({ x, onDone }: { x: Experiment; onDone: () => void }) {
  const { session } = useConsole();
  const base = `/v1/admin/experiments/${x.exp_id}`;
  const to = nextTraffic[x.traffic_percent];
  return (
    <Space>
      {to && (
        <ConfirmAction
          label={`推进到 ${to}%`}
          title={`把实验组流量从 ${x.traffic_percent}% 推进到 ${to}%？`}
          onSubmit={async (key) => {
            await request<Experiment>(session, `${base}/ramp`, { body: { traffic_percent: to }, idempotencyKey: key });
            return `已推进到 ${to}%`;
          }}
          onDone={onDone}
        />
      )}
      <ReasonAction
        label="停止"
        title="停止实验：全部流量回到基线"
        danger
        onSubmit={async (reason, key) => {
          await request<Experiment>(session, `${base}/stop`, { body: { reason }, idempotencyKey: key });
          return "已停止";
        }}
        onDone={onDone}
      />
    </Space>
  );
}

const guardrailLabel: Record<string, string> = {
  refused_rate: "拒答率",
  p95_latency_ms: "p95 耗时（ms）",
  mean_tokens: "单次 token",
};

function pct(n: number): string {
  return `${(n * 100).toFixed(1)}%`;
}

function AnalysisView({ expId }: { expId: string }) {
  const { session } = useConsole();
  const a = useLoad(
    (signal) => request<ExperimentAnalysis>(session, `/v1/admin/experiments/${expId}/analysis`, { signal }),
    expId,
  );
  if (a.loading) return <Spin />;
  if (a.error !== undefined) return <ErrorAlert error={a.error} onRetry={a.reload} />;
  const r = a.data;
  if (!r) return null;
  const c = r.conclusion;
  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Space>
        样本进度
        <Progress percent={Math.round(r.progress * 100)} style={{ width: 240 }} />
        <span>
          数据范围 {fmtTime(r.data_since)} – {fmtTime(r.data_until)}
        </span>
      </Space>
      {c ? (
        <Alert
          type={c.significant ? (c.improved ? "success" : "error") : "info"}
          showIcon
          message={c.significant ? (c.improved ? "实验组显著改善" : "实验组显著变差") : "差异不显著（p ≥ 0.05）"}
          description={`差值 ${pct(c.delta)}，95% 置信区间 [${pct(c.ci_low)}, ${pct(c.ci_high)}]，p = ${c.p_value.toFixed(4)}`}
        />
      ) : (
        <Alert type="info" showIcon message="还没达到样本量，暂不给结论" />
      )}
      <Table<ArmSummary>
        rowKey="arm"
        size="small"
        pagination={false}
        dataSource={[r.control, r.treatment]}
        columns={[
          { title: "组", dataIndex: "arm", render: (x: string) => (x === "control" ? "对照组" : "实验组") },
          { title: "用户", dataIndex: "users" },
          { title: "回答", dataIndex: "answers" },
          { title: "计入主要指标的用户", dataIndex: "metric_users" },
          { title: "主要指标", dataIndex: "metric_mean", render: pct },
        ]}
      />
      <Descriptions size="small" title="护栏" column={3} bordered>
        {r.guardrails.map((g) => (
          <Descriptions.Item key={g.name} label={guardrailLabel[g.name] ?? g.name}>
            对照 {g.control.toFixed(3)} / 实验 {g.treatment.toFixed(3)}{" "}
            {g.breached ? <Tag color="red">超出</Tag> : <Tag color="green">正常</Tag>}
          </Descriptions.Item>
        ))}
      </Descriptions>
    </Space>
  );
}

interface CreateForm {
  tenant: string;
  hypothesis: string;
  primary_metric: Experiment["primary_metric"];
  baseline_rate: number;
  min_detectable_effect: number;
  support_threshold?: number;
  min_coverage?: number;
  config_versions?: string[];
}

// 立项：一个主要指标、基线值与最小可检测差异（据此算样本量）、实验组替换的阈值与配置版本；从 5% 流量开始。
// 配置版本须已单独过评测门禁（governance 立项时核对，没过的返回 gate_failed）
function CreateButton({ onDone }: { onDone: () => void }) {
  const { session, config } = useConsole();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<CreateForm>();
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const v = await form.validateFields();
    const body: ExperimentRequest = {
      tenant: v.tenant,
      hypothesis: v.hypothesis.trim(),
      primary_metric: v.primary_metric,
      baseline_rate: v.baseline_rate,
      min_detectable_effect: v.min_detectable_effect,
      overrides: {
        support_threshold: v.support_threshold ?? undefined,
        min_coverage: v.min_coverage ?? undefined,
        config_versions: v.config_versions?.length ? v.config_versions : undefined,
      },
    };
    const o = body.overrides;
    if (o.support_threshold === undefined && o.min_coverage === undefined && !o.config_versions) {
      void message.error("至少替换一项：阈值或配置版本");
      return;
    }
    setBusy(true);
    try {
      const x = await request<Experiment>(session, "/v1/admin/experiments", { body, idempotencyKey: keyRef.current });
      keyRef.current = newIdempotencyKey();
      void message.success(`已立项 ${x.exp_id}，每组需要 ${x.sample_size_per_arm} 位用户`);
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
        立项
      </Button>
      <Modal
        open={open}
        title="新实验（从 5% 流量开始）"
        okText="立项"
        cancelText="取消"
        confirmLoading={busy}
        onOk={() => void submit()}
        onCancel={() => setOpen(false)}
      >
        <Form form={form} layout="vertical" initialValues={{ primary_metric: "useful_rate" }}>
          <Form.Item name="tenant" label="租户" rules={[{ required: true }]}>
            <Select options={config.tenants.map((t) => ({ value: t.id, label: t.name }))} />
          </Form.Item>
          <Form.Item name="hypothesis" label="假设与预期效果" rules={[{ required: true, whitespace: true, max: 1000 }]}>
            <Input.TextArea autoSize={{ minRows: 2 }} maxLength={1000} showCount />
          </Form.Item>
          <Form.Item name="primary_metric" label="主要指标" rules={[{ required: true }]}>
            <Select options={Object.entries(metricLabel).map(([value, label]) => ({ value, label }))} />
          </Form.Item>
          <Space>
            <Form.Item name="baseline_rate" label="主要指标当前值" rules={[{ required: true }]}>
              <InputNumber min={0.001} max={0.999} step={0.01} />
            </Form.Item>
            <Form.Item name="min_detectable_effect" label="最小可检测差异" rules={[{ required: true }]}>
              <InputNumber min={0.001} max={0.5} step={0.01} />
            </Form.Item>
          </Space>
          <Space>
            <Form.Item name="support_threshold" label="实验组句支持度门槛" extra="0.3–0.9，不填沿用基线">
              <InputNumber min={0.3} max={0.9} step={0.05} />
            </Form.Item>
            <Form.Item name="min_coverage" label="实验组引用覆盖率门槛" extra="0.5–1，不填沿用基线">
              <InputNumber min={0.5} max={1} step={0.05} />
            </Form.Item>
          </Space>
          <Form.Item
            name="config_versions"
            label="实验组替换的配置版本"
            extra="cfv_ 开头，最多 4 个：该租户的 pipeline、routing（生成模型与置信度权重）、回答模板各至多一个，须已通过评测门禁"
            rules={[
              {
                validator: async (_, v?: string[]) => {
                  if ((v ?? []).length > 4) throw new Error("最多 4 个");
                  if ((v ?? []).some((x) => !configVersion.test(x))) throw new Error("须为 cfv_ 版本");
                },
              },
            ]}
          >
            <Select mode="tags" tokenSeparators={[",", " "]} open={false} placeholder="粘贴配置版本 ID" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
