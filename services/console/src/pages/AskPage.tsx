import { ClearOutlined, DeleteOutlined, SendOutlined, StopOutlined, WarningOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Collapse,
  Descriptions,
  Empty,
  Input,
  List,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
  message,
} from "antd";
import { useRef, useState } from "react";
import {
  applyEvent,
  classificationText,
  confidenceColor,
  confidenceText,
  finish,
  initialAnswer,
  intentText,
  plainText,
  statusText,
  type AnswerState,
} from "../answer";
import { describe, newIdempotencyKey, request, send } from "../api/client";
import { readSSE } from "../api/sse";
import type { Citation, FeedbackRating, StreamSentence, Why } from "../api/types";
import { ErrorAlert } from "../components/ErrorAlert";
import { PreviewDrawer } from "../components/PreviewDrawer";
import { useConsole } from "../context";

interface Turn {
  id: number;
  question: string;
  answer: AnswerState;
}

export function AskPage() {
  const { session, tenant } = useConsole();
  const [question, setQuestion] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [conversationId, setConversationId] = useState<string>();
  const [preview, setPreview] = useState<Citation>();
  const [deleting, setDeleting] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const busy = turns.some((t) => t.answer.phase === "streaming");

  const update = (id: number, f: (a: AnswerState) => AnswerState) =>
    setTurns((ts) => ts.map((t) => (t.id === id ? { ...t, answer: f(t.answer) } : t)));

  // 删除服务端保存的会话（摘要、最近的问题与上轮引用）；回答记录与审计不受影响
  async function removeConversation(id: string) {
    setDeleting(true);
    try {
      await send(session, `/v1/conversations/${encodeURIComponent(id)}`, { method: "DELETE", withTenant: true });
      setTurns([]);
      setConversationId(undefined);
      void message.success("会话已删除");
    } catch (err) {
      void message.error(`删除会话失败：${describe(err)}`);
    } finally {
      setDeleting(false);
    }
  }

  async function ask(q: string) {
    const text = q.trim();
    if (!text || busy) return;
    const id = Date.now();
    setTurns((ts) => [...ts, { id, question: text, answer: initialAnswer }]);
    setQuestion("");
    const ctl = new AbortController();
    abortRef.current = ctl;
    try {
      const resp = await send(session, "/v1/answers", {
        body: { question: text, stream: true, conversation_id: conversationId },
        withTenant: true,
        accept: "text/event-stream",
        signal: ctl.signal,
      });
      await readSSE(resp, (ev) => {
        if (ev.event === "meta") {
          const meta = JSON.parse(ev.data) as { conversation_id?: string };
          if (meta.conversation_id) setConversationId(meta.conversation_id);
        }
        update(id, (a) => applyEvent(a, ev));
      });
      update(id, finish);
    } catch (e) {
      const aborted = e instanceof DOMException && e.name === "AbortError";
      update(id, (a) => ({ ...a, phase: "error", error: aborted ? "已停止" : describe(e) }));
    } finally {
      abortRef.current = null;
    }
  }

  return (
    <Space direction="vertical" size="large" style={{ width: "100%", maxWidth: 960 }}>
      {turns.length === 0 && (
        <Empty
          description={
            <span>
              以当前用户的权限检索租户 <Tag>{tenant}</Tag>
              的知识，回答的每一句都附带引用。试试“个人消费贷款的额度上限是多少？”
            </span>
          }
        />
      )}
      {turns.map((t) => (
        <TurnView key={t.id} turn={t} onPreview={setPreview} onRetry={() => ask(t.question)} />
      ))}
      <Card size="small">
        <Space.Compact style={{ width: "100%" }}>
          <Input.TextArea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onPressEnter={(e) => {
              if (!e.shiftKey) {
                e.preventDefault();
                void ask(question);
              }
            }}
            placeholder="输入问题，Enter 发送，Shift+Enter 换行"
            autoSize={{ minRows: 2, maxRows: 6 }}
            maxLength={2000}
            disabled={busy}
          />
        </Space.Compact>
        <Space style={{ marginTop: 8 }}>
          {busy ? (
            <Button icon={<StopOutlined />} onClick={() => abortRef.current?.abort()}>
              停止
            </Button>
          ) : (
            <Button
              type="primary"
              icon={<SendOutlined />}
              onClick={() => void ask(question)}
              disabled={!question.trim()}
            >
              提问
            </Button>
          )}
          <Button
            icon={<ClearOutlined />}
            disabled={busy || turns.length === 0}
            onClick={() => {
              setTurns([]);
              setConversationId(undefined);
            }}
          >
            新会话
          </Button>
          {conversationId && (
            <Button
              icon={<DeleteOutlined />}
              loading={deleting}
              disabled={busy}
              onClick={() => void removeConversation(conversationId)}
            >
              删除会话
            </Button>
          )}
          {conversationId && <Typography.Text type="secondary">会话 {conversationId}</Typography.Text>}
        </Space>
      </Card>
      <PreviewDrawer citation={preview} onClose={() => setPreview(undefined)} />
    </Space>
  );
}

function TurnView({ turn, onPreview, onRetry }: { turn: Turn; onPreview: (c: Citation) => void; onRetry: () => void }) {
  const a = turn.answer;
  const byRef = new Map(a.citations.map((c) => [c.ref_id, c]));
  return (
    <Card
      title={<Typography.Text strong>{turn.question}</Typography.Text>}
      extra={<AnswerTags answer={a} />}
      size="small"
    >
      {a.phase === "streaming" && a.sentences.length === 0 && <Spin tip="检索与生成中…" />}
      <Sentences sentences={a.sentences} byRef={byRef} onPreview={onPreview} />
      {a.final?.status === "insufficient_evidence" && (
        <Alert type="warning" showIcon message="证据不足：已有资料不足以完整回答，以上只列出有依据的部分" />
      )}
      {a.final?.status === "refused" && <Alert type="info" showIcon message="问题超出知识范围，未检索、未回答" />}
      {a.final?.confidence?.level === "low" && (
        <Alert type="warning" showIcon message="置信度低，建议点开引用核实原文" style={{ marginTop: 8 }} />
      )}
      {a.phase === "error" && <ErrorAlert error={new Error(a.error)} onRetry={onRetry} />}
      {a.citations.length > 0 && <Citations citations={a.citations} onPreview={onPreview} />}
      {a.final && <WhyView why={a.final.why} />}
      {a.phase === "done" && a.meta && a.final?.status !== "refused" && <Feedback answerId={a.meta.answer_id} />}
    </Card>
  );
}

function AnswerTags({ answer }: { answer: AnswerState }) {
  const f = answer.final;
  if (!f) return answer.phase === "streaming" ? <Tag color="processing">回答中</Tag> : null;
  return (
    <Space size={4}>
      <Tag color={f.status === "answered" ? "success" : "warning"}>{statusText[f.status]}</Tag>
      {f.confidence && (
        <Tooltip title={`置信度 ${f.confidence.score.toFixed(2)}（覆盖率 ${f.confidence.coverage.toFixed(2)}）`}>
          <Tag color={confidenceColor[f.confidence.level]}>置信度{confidenceText[f.confidence.level]}</Tag>
        </Tooltip>
      )}
    </Space>
  );
}

function Sentences({
  sentences,
  byRef,
  onPreview,
}: {
  sentences: StreamSentence[];
  byRef: Map<number, Citation>;
  onPreview: (c: Citation) => void;
}) {
  // 按块拼装：同一 block_id 的句子放在一起，列表项与步骤单独成行
  const blocks: { id: string; type: string; items: StreamSentence[] }[] = [];
  for (const s of sentences) {
    const last = blocks[blocks.length - 1];
    if (last && last.id === s.block_id) last.items.push(s);
    else blocks.push({ id: s.block_id, type: s.block_type, items: [s] });
  }
  return (
    <div>
      {blocks.map((b) => {
        const content = b.items.map((s, i) => (
          <span key={i}>
            {plainText(s)}
            {s.ref_ids.map((r) => {
              const c = byRef.get(r);
              return (
                <sup key={r}>
                  <a onClick={() => c && onPreview(c)} title={c?.title}>
                    [{r}]
                  </a>
                </sup>
              );
            })}
          </span>
        ));
        if (b.type === "heading")
          return (
            <Typography.Title key={b.id} level={5}>
              {content}
            </Typography.Title>
          );
        if (b.type === "list_item" || b.type === "step") return <li key={b.id}>{content}</li>;
        return <Typography.Paragraph key={b.id}>{content}</Typography.Paragraph>;
      })}
    </div>
  );
}

function Citations({ citations, onPreview }: { citations: Citation[]; onPreview: (c: Citation) => void }) {
  return (
    <List
      size="small"
      header={<Typography.Text type="secondary">引用</Typography.Text>}
      dataSource={citations}
      renderItem={(c) => (
        <List.Item
          actions={[
            <a key="p" onClick={() => onPreview(c)}>
              预览原文
            </a>,
          ]}
        >
          <List.Item.Meta
            title={
              <Space wrap size={4}>
                <span>[{c.ref_id}]</span>
                <span>{c.title}</span>
                <Tag>{classificationText[c.classification] ?? c.classification}</Tag>
                <Tag>{c.source_type}</Tag>
                {c.injection_flag && (
                  <Tag color="red" icon={<WarningOutlined />}>
                    可疑内容
                  </Tag>
                )}
              </Space>
            }
            description={<Typography.Text type="secondary">“{c.quote}”</Typography.Text>}
          />
        </List.Item>
      )}
    />
  );
}

function WhyView({ why }: { why: Why }) {
  const rows: [string, string | undefined][] = [
    ["意图", why.intent && (intentText[why.intent] ?? why.intent)],
    ["索引版本", why.index_version],
    ["策略版本", why.policy_version],
    ["召回", why.retrievers?.join("、")],
    ["嵌入模型", why.embedding_model],
    ["精排模型", why.rerank_model],
    ["生成模型", why.generation_model],
    ["提示词版本", why.prompt_version],
    ["校验模型", why.nli_model],
    ["支持度阈值", why.support_threshold?.toString()],
    ["实验", why.experiment_id && `${why.experiment_id}（${why.experiment_arm}）`],
  ];
  return (
    <Collapse
      size="small"
      style={{ marginTop: 8 }}
      items={[
        {
          key: "why",
          label: "为什么是这个回答（why）",
          children: (
            <Space direction="vertical" style={{ width: "100%" }}>
              <Descriptions size="small" column={2} bordered>
                {rows
                  .filter(([, v]) => v)
                  .map(([k, v]) => (
                    <Descriptions.Item key={k} label={k}>
                      {v}
                    </Descriptions.Item>
                  ))}
              </Descriptions>
              {why.based_on_summaries && <Alert type="info" showIcon message="要点基于入库时生成的文档摘要归纳" />}
              {(why.agent_rounds ?? []).map((r) => (
                <Card key={r.round} size="small" title={`多步检索 第 ${r.round} 轮`}>
                  {r.calls.map((c) => (
                    <div key={c.sub_question_id}>
                      <Tag color={c.covered ? "green" : "default"}>{c.tool}</Tag>
                      {c.sub_question}（{c.chunk_ids.length} 条证据，{c.elapsed_ms} ms）
                    </div>
                  ))}
                </Card>
              ))}
              {(why.graph_path ?? []).map((p) => (
                <div key={p.ref_id}>
                  <Typography.Text type="secondary">引用 [{p.ref_id}] 的图路径：</Typography.Text>
                  {p.edges.map((e, i) => (
                    <Tag key={i}>
                      {e.from_entity} —{e.relation_type}→ {e.to_entity}
                    </Tag>
                  ))}
                </div>
              ))}
              {(why.degraded ?? []).map((d, i) => (
                <Alert key={i} type="warning" showIcon message={`降级：${d.stage}（${d.reason}）`} />
              ))}
            </Space>
          ),
        },
      ]}
    />
  );
}

const ratings: { value: FeedbackRating; label: string }[] = [
  { value: "useful", label: "有用" },
  { value: "wrong", label: "有误" },
  { value: "bad_citation", label: "引用不对" },
];

function Feedback({ answerId }: { answerId: string }) {
  const { session } = useConsole();
  const [sent, setSent] = useState<FeedbackRating>();
  const [pending, setPending] = useState<FeedbackRating>();
  // 同一次回答的反馈重试沿用同一个幂等键
  const keyRef = useRef(newIdempotencyKey());
  async function submit(rating: FeedbackRating) {
    setPending(rating);
    try {
      await request(session, `/v1/answers/${answerId}/feedback`, {
        body: { rating },
        withTenant: true,
        idempotencyKey: keyRef.current,
      });
      setSent(rating);
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setPending(undefined);
    }
  }
  if (sent)
    return <Typography.Text type="secondary">已反馈：{ratings.find((r) => r.value === sent)?.label}</Typography.Text>;
  return (
    <Space style={{ marginTop: 8 }}>
      <Typography.Text type="secondary">这个回答：</Typography.Text>
      {ratings.map((r) => (
        <Button
          key={r.value}
          size="small"
          loading={pending === r.value}
          disabled={!!pending}
          onClick={() => void submit(r.value)}
        >
          {r.label}
        </Button>
      ))}
    </Space>
  );
}
