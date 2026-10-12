import { Button, Card, Select, Space, Table, Tabs, Tag, Typography, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey, request } from "../api/client";
import type { FeedbackCase, FeedbackCaseList, GoldenQuestion, GoldenQuestionList } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { ReasonAction } from "../components/ReasonAction";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const statusLabel: Record<string, string> = {
  answered: "应回答",
  insufficient_evidence: "证据不足",
  refused: "应拒答",
};

// 评测集（evaluation 详设第 3 节，当前租户）：复核候选题、处理用户的负反馈案例。
// 候选题要业务专家与平台各一人复核才进评测集；负反馈由业务专家出题后标为已转题，或驳回
export function GoldenPage() {
  const { tenant } = useConsole();
  return (
    <Card title={`评测集：${tenant}`}>
      <Tabs
        items={[
          { key: "candidates", label: "候选题复核", children: <Candidates key={tenant} /> },
          { key: "feedback", label: "负反馈案例", children: <FeedbackCases key={tenant} /> },
        ]}
      />
    </Card>
  );
}

function Candidates() {
  const { session, tenant } = useConsole();
  const path = `/v1/admin/golden-sets/${encodeURIComponent(tenant)}/questions`;
  const list = useLoad(
    (signal) => request<GoldenQuestionList>(session, path, { query: { include_candidates: "true" }, signal }),
    tenant,
  );
  const pending = (list.data?.questions ?? []).filter((q) => q.status === "pending");
  if (list.error !== undefined) return <ErrorAlert error={list.error} onRetry={list.reload} />;
  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Typography.Text type="secondary">
        评测集版本：{list.data?.golden_versions.join("、") || "—"}。复核人不能是提交人。
      </Typography.Text>
      <Table<GoldenQuestion>
        rowKey="question_id"
        size="small"
        loading={list.loading}
        dataSource={pending}
        pagination={{ pageSize: 20 }}
        locale={{ emptyText: "没有待复核的候选题" }}
        expandable={{
          expandedRowRender: (q) => (
            <Space direction="vertical">
              <span>要点：{q.expected.key_points.join("；") || "—"}</span>
              <span>必须引用：{q.expected.must_cite.join("、") || "—"}</span>
              <span>不得引用：{q.expected.must_not_cite.join("、") || "—"}</span>
            </Space>
          ),
        }}
        columns={[
          { title: "题号", dataIndex: "question_id" },
          { title: "分组", dataIndex: "bucket" },
          { title: "提问身份", dataIndex: "asker" },
          { title: "问题", dataIndex: "question" },
          { title: "预期", render: (_, q) => statusLabel[q.expected.status] ?? q.expected.status },
          { title: "来源", dataIndex: "origin" },
          { title: "提交人", dataIndex: "submitted_by" },
          {
            title: "已复核",
            render: (_, q) =>
              [q.expert_reviewer && `业务：${q.expert_reviewer}`, q.platform_reviewer && `平台：${q.platform_reviewer}`]
                .filter(Boolean)
                .join("，") || "—",
          },
          {
            title: "",
            render: (_, q) => <ReviewActions path={`${path}/${q.question_id}/review`} onDone={list.reload} />,
          },
        ]}
      />
    </Space>
  );
}

function ReviewActions({ path, onDone }: { path: string; onDone: () => void }) {
  const { session } = useConsole();
  const [busy, setBusy] = useState(false);
  const keyRef = useRef(newIdempotencyKey());
  async function approve() {
    setBusy(true);
    try {
      const q = await request<GoldenQuestion>(session, path, {
        body: { approved: true },
        idempotencyKey: keyRef.current,
      });
      keyRef.current = newIdempotencyKey();
      void message.success(q.status === "accepted" ? "已通过，进入评测集" : "已记下复核，还需另一方复核");
      onDone();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Space>
      <Button size="small" type="primary" loading={busy} onClick={() => void approve()}>
        通过
      </Button>
      <ReasonAction
        label="驳回"
        title="驳回候选题"
        danger
        onSubmit={async (reason, key) => {
          await request<GoldenQuestion>(session, path, { body: { approved: false, reason }, idempotencyKey: key });
          return "已驳回";
        }}
        onDone={onDone}
      />
    </Space>
  );
}

const caseStatus: Record<string, { label: string; color: string }> = {
  open: { label: "待处理", color: "orange" },
  converted: { label: "已转题", color: "green" },
  dismissed: { label: "已驳回", color: "default" },
};

function FeedbackCases() {
  const { session, tenant } = useConsole();
  const [status, setStatus] = useState("open");
  const path = `/v1/admin/golden-sets/${encodeURIComponent(tenant)}/feedback-cases`;
  const list = useLoad(
    (signal) => request<FeedbackCaseList>(session, path, { query: { status }, signal }),
    `${tenant}/${status}`,
  );
  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Space>
        <Typography.Text type="secondary">每日抽样任务登记前一天的“有误”“引用不对”反馈；问题已做掩码。</Typography.Text>
        <Select
          style={{ width: 120 }}
          value={status}
          onChange={setStatus}
          options={Object.entries(caseStatus).map(([value, s]) => ({ value, label: s.label }))}
        />
      </Space>
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <Table<FeedbackCase>
          rowKey="answer_id"
          size="small"
          loading={list.loading}
          dataSource={list.data?.cases ?? []}
          pagination={{ pageSize: 20 }}
          locale={{ emptyText: "没有这类案例" }}
          expandable={{
            expandedRowRender: (c) => (
              <Space direction="vertical">
                <span>说明：{c.comments.join("；") || "—"}</span>
                <span>引用的文档：{c.cited_titles.join("、") || "—"}</span>
                <span>回答 ID：{c.answer_id}</span>
                {c.resolved_by && (
                  <span>
                    处理：{c.resolved_by}，{fmtTime(c.resolved_at)}，{c.resolution_note}
                  </span>
                )}
              </Space>
            ),
          }}
          columns={[
            { title: "回答时间", dataIndex: "answered_at", render: fmtTime },
            { title: "问题（掩码）", dataIndex: "masked_question" },
            {
              title: "反馈",
              dataIndex: "ratings",
              render: (r: string[]) =>
                r.map((x) => <Tag key={x}>{x === "wrong" ? "有误" : x === "bad_citation" ? "引用不对" : x}</Tag>),
            },
            {
              title: "状态",
              dataIndex: "status",
              render: (s: string) => <Tag color={caseStatus[s]?.color}>{caseStatus[s]?.label ?? s}</Tag>,
            },
            {
              title: "",
              render: (_, c) =>
                c.status === "open" ? (
                  <Space>
                    <ReasonAction
                      label="已转题"
                      title="标为已转题"
                      fieldLabel="候选题 ID"
                      onSubmit={async (note, key) => {
                        await request<FeedbackCase>(session, `${path}/${c.answer_id}/resolve`, {
                          body: { resolution: "converted", note },
                          idempotencyKey: key,
                        });
                      }}
                      onDone={list.reload}
                    />
                    <ReasonAction
                      label="驳回"
                      title="驳回这条反馈"
                      danger
                      onSubmit={async (note, key) => {
                        await request<FeedbackCase>(session, `${path}/${c.answer_id}/resolve`, {
                          body: { resolution: "dismissed", note },
                          idempotencyKey: key,
                        });
                      }}
                      onDone={list.reload}
                    />
                  </Space>
                ) : null,
            },
          ]}
        />
      )}
    </Space>
  );
}
