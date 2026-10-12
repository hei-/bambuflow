import { Card, Descriptions, Drawer, List, Space, Table, Tag, Typography } from "antd";
import { useState } from "react";
import { request } from "../api/client";
import type { EvalGate, EvalGateList } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { useConsole } from "../context";
import { fmtTime } from "../format";
import { useLoad } from "../useLoad";

const conclusionColor = { passed: "green", failed: "red", indeterminate: "orange" } as const;
const conclusionText = { passed: "通过", failed: "不通过", indeterminate: "无法判定" } as const;
const stageText = { none: "未达试点", pilot: "满足试点", general: "满足全量" } as const;
const stageColor = { none: "default", pilot: "blue", general: "green" } as const;

const triggerText: Record<string, string> = {
  ci: "CI",
  config_release: "配置发布",
  index_publish: "增量发布",
  index_rebuild: "全量重建",
  scheduled: "定时",
  manual: "手动",
};

// 评测门禁报告（evaluation 详设 8.2）
export function EvalGatesPage() {
  const { session } = useConsole();
  const list = useLoad(
    (signal) => request<EvalGateList>(session, "/v1/admin/eval-gates", { query: { page_size: 50 }, signal }),
    "gates",
  );
  const [open, setOpen] = useState<EvalGate>();
  return (
    <Card title="评测门禁">
      {list.error !== undefined ? (
        <ErrorAlert error={list.error} onRetry={list.reload} />
      ) : (
        <Table<EvalGate>
          rowKey="gate_id"
          size="small"
          loading={list.loading}
          dataSource={list.data?.gates ?? []}
          locale={{ emptyText: "还没有门禁记录" }}
          onRow={(g) => ({ onClick: () => setOpen(g), style: { cursor: "pointer" } })}
          columns={[
            { title: "时间", dataIndex: "created_at", render: fmtTime },
            { title: "触发", dataIndex: "trigger", render: (t: string) => triggerText[t] ?? t },
            {
              title: "结论",
              dataIndex: "conclusion",
              render: (c: EvalGate["conclusion"]) => <Tag color={conclusionColor[c]}>{conclusionText[c]}</Tag>,
            },
            { title: "租户", dataIndex: "tenants", render: (t: string[]) => t.join("、") },
            { title: "评测集", render: (_, g) => `${g.golden_set}@${g.golden_version}` },
            { title: "原因", dataIndex: "reasons", render: (r: string[]) => r.join("；") || "—" },
          ]}
        />
      )}
      <Drawer open={!!open} onClose={() => setOpen(undefined)} width={720} title={open?.gate_id}>
        {open && <GateDetail gate={open} />}
      </Drawer>
    </Card>
  );
}

function GateDetail({ gate }: { gate: EvalGate }) {
  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Descriptions size="small" column={1} bordered>
        <Descriptions.Item label="结论">
          <Tag color={conclusionColor[gate.conclusion]}>{conclusionText[gate.conclusion]}</Tag>
        </Descriptions.Item>
        {Object.entries(gate.metrics).map(([k, v]) => (
          <Descriptions.Item key={k} label={k}>
            {v === null ? "—" : v.toFixed(3)}
          </Descriptions.Item>
        ))}
        {gate.unstable.length > 0 && <Descriptions.Item label="不稳定">{gate.unstable.join("、")}</Descriptions.Item>}
        {Object.entries(gate.config).map(([k, v]) => (
          <Descriptions.Item key={k} label={k}>
            {v.join("、")}
          </Descriptions.Item>
        ))}
      </Descriptions>
      {gate.readiness.length > 0 && (
        <List
          size="small"
          header={
            <Typography.Text strong>
              各租户评测集（分阶段接入：试点 ≥ 100 题，全量 ≥ 300 题且双人标注 ≥ 150）
            </Typography.Text>
          }
          dataSource={gate.readiness}
          renderItem={(r) => (
            <List.Item>
              <Space direction="vertical" size={2}>
                <Space wrap>
                  <Typography.Text strong>{r.tenant}</Typography.Text>
                  <Tag color={stageColor[r.stage]}>{stageText[r.stage]}</Tag>
                  <span>
                    已审核 {r.counted} 道（未审核 {r.unreviewed}），应拒答 {(r.refuse_share * 100).toFixed(0)}%，合成题{" "}
                    {(r.synthetic_share * 100).toFixed(0)}%，双人标注 {r.double_reviewed} 道
                  </span>
                </Space>
                {r.gaps.length > 0 && <Typography.Text type="secondary">还差：{r.gaps.join("；")}</Typography.Text>}
              </Space>
            </List.Item>
          )}
        />
      )}
      {gate.report_markdown && (
        <Typography.Paragraph>
          <pre style={{ whiteSpace: "pre-wrap" }}>{gate.report_markdown}</pre>
        </Typography.Paragraph>
      )}
    </Space>
  );
}
