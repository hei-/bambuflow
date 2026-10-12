import { Alert, Button, Card, Form, Input, List, Select, Space, Tag, Typography } from "antd";
import { useState } from "react";
import { ApiError, describe, newIdempotencyKey, request } from "../api/client";
import type { ChangeRequest, ConfigVersion, ConfigVersionList } from "../api/types";
import { useConsole } from "../context";
import { fmtTime } from "../format";

// Git 类配置对象（governance 详设 3.1，平台管理员）：pipeline、routing、策略包的一层、回答模板。
// 在当前生效内容上修改后提交：governance 校验内容，写成合并请求，在 Git 里评审、跑评测门禁，合并后由 CI 发布生效；
// 这里不能直接审批生效
type ObjectKind = "pipeline" | "routing" | "policy" | "prompt";

const OBJECTS: { value: ObjectKind; label: string; scopeHint: string }[] = [
  { value: "pipeline", label: "pipeline（租户的检索计划、回答阈值与租户规则）", scopeHint: "租户名，如 credit" },
  { value: "routing", label: "routing（全局的置信度权重）", scopeHint: "global" },
  { value: "policy", label: "策略包的一层", scopeHint: "global、tenant:<租户> 或 source:<数据源>" },
  { value: "prompt", label: "回答模板", scopeHint: "answer 或 agent-answer" },
];

interface Loaded {
  object: ObjectKind;
  scope: string;
  active?: ConfigVersion;
}

export function ConfigObjectsPage() {
  const { session } = useConsole();
  const [object, setObject] = useState<ObjectKind>("pipeline");
  const [scope, setScope] = useState("");
  const [loaded, setLoaded] = useState<Loaded>();
  const [content, setContent] = useState("");
  const [reason, setReason] = useState("");
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string>();
  const [submitted, setSubmitted] = useState<ChangeRequest>();
  const hint = OBJECTS.find((o) => o.value === object)?.scopeHint ?? "";

  async function load() {
    setLoading(true);
    setError(undefined);
    setSubmitted(undefined);
    try {
      const q = new URLSearchParams({ object, scope: scope.trim() });
      const r = await request<ConfigVersionList>(session, `/v1/admin/config-versions?${q}`);
      const active = r.versions.find((v) => v.active);
      setLoaded({ object, scope: scope.trim(), active });
      setContent(active?.content ?? "");
    } catch (e) {
      // 还没有任何版本时 governance 返回 404：按新建处理
      if (e instanceof ApiError && e.status === 404) {
        setLoaded({ object, scope: scope.trim() });
        setContent("");
      } else {
        setError(describe(e));
      }
    } finally {
      setLoading(false);
    }
  }

  async function submit() {
    if (!loaded) return;
    setSubmitting(true);
    setError(undefined);
    try {
      const c = await request<ChangeRequest>(session, "/v1/admin/change-requests", {
        method: "POST",
        idempotencyKey: newIdempotencyKey(),
        body: {
          object: loaded.object,
          scope: loaded.scope,
          base_version_id: loaded.active?.version_id ?? "",
          content,
          reason: reason.trim(),
          gate_id: "",
        },
      });
      setSubmitted(c);
    } catch (e) {
      setError(describe(e));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Card title="配置对象">
        <Typography.Paragraph type="secondary">
          这些配置的事实来源在 Git：提交后写成合并请求，在 Git 里评审、跑评测门禁，合并后由 CI 发布生效。
        </Typography.Paragraph>
        <Space wrap>
          <Select<ObjectKind>
            value={object}
            style={{ width: 360 }}
            options={OBJECTS.map((o) => ({ value: o.value, label: o.label }))}
            onChange={(v) => {
              setObject(v);
              setLoaded(undefined);
            }}
          />
          <Input
            placeholder={hint}
            value={scope}
            style={{ width: 260 }}
            onChange={(e) => {
              setScope(e.target.value);
              setLoaded(undefined);
            }}
          />
          <Button onClick={() => void load()} loading={loading} disabled={!scope.trim()}>
            读取生效版本
          </Button>
        </Space>
      </Card>
      {error && <Alert type="error" showIcon message={error} />}
      {loaded && (
        <Card
          title={
            loaded.active
              ? `${loaded.object} ${loaded.scope}：生效版本 #${loaded.active.number}（${fmtTime(loaded.active.created_at)}，${loaded.active.created_by}）`
              : `${loaded.object} ${loaded.scope}：还没有生效版本，将新建`
          }
        >
          {loaded.active?.widened && loaded.active.widened.length > 0 && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 12 }}
              message="这个生效版本发布时，以下主体的可见范围变大"
              description={
                <List size="small" dataSource={loaded.active.widened} renderItem={(w) => <List.Item>{w}</List.Item>} />
              }
            />
          )}
          <Form layout="vertical">
            <Form.Item label="内容">
              <Input.TextArea
                aria-label="内容"
                value={content}
                autoSize={{ minRows: 8, maxRows: 30 }}
                style={{ fontFamily: "monospace" }}
                onChange={(e) => setContent(e.target.value)}
              />
            </Form.Item>
            <Form.Item label="修改原因（必填，写进合并请求）">
              <Input aria-label="修改原因" value={reason} maxLength={500} onChange={(e) => setReason(e.target.value)} />
            </Form.Item>
            <Button
              type="primary"
              loading={submitting}
              disabled={!content.trim() || !reason.trim() || content === loaded.active?.content}
              onClick={() => void submit()}
            >
              提交合并请求
            </Button>
          </Form>
        </Card>
      )}
      {submitted && (
        <Alert
          type="success"
          showIcon
          message={
            <span>
              已写成合并请求 <Tag>{submitted.chg_id}</Tag>
              {submitted.merge_request_url && (
                <a href={submitted.merge_request_url} target="_blank" rel="noreferrer">
                  {submitted.merge_request_url}
                </a>
              )}
            </span>
          }
          description="在 Git 里评审并跑评测门禁，合并后由 CI 发布生效。"
        />
      )}
    </Space>
  );
}
