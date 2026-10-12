import { Drawer, Space, Spin, Tag, Typography } from "antd";
import { request } from "../api/client";
import type { Citation, DocumentPreview } from "../api/types";
import { classificationText } from "../answer";
import { useConsole } from "../context";
import { useLoad } from "../useLoad";
import { ErrorAlert } from "./ErrorAlert";

// 引用的原文预览：经 retrieval 按当前用户的权限返回片段与高亮位置（GET /v1/documents/{doc_id}/preview）
export function PreviewDrawer({ citation, onClose }: { citation?: Citation; onClose: () => void }) {
  return (
    <Drawer open={!!citation} onClose={onClose} width={640} title={citation?.title ?? "原文"} destroyOnHidden>
      {citation && <PreviewBody citation={citation} />}
    </Drawer>
  );
}

function PreviewBody({ citation }: { citation: Citation }) {
  const { session } = useConsole();
  const { data, error, loading, reload } = useLoad(
    (signal) =>
      request<DocumentPreview>(session, `/v1/documents/${encodeURIComponent(citation.doc_id)}/preview`, {
        query: { chunk_id: citation.chunk_id, quote: citation.quote },
        withTenant: true,
        signal,
      }),
    citation.chunk_id,
  );
  if (loading) return <Spin />;
  if (error) return <ErrorAlert error={error} onRetry={reload} />;
  if (!data) return null;
  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Space wrap>
        <Tag>{classificationText[data.classification] ?? data.classification}</Tag>
        <Tag>{data.source_type}</Tag>
        <Typography.Text type="secondary">版本 {data.doc_version}</Typography.Text>
        <Typography.Text type="secondary">索引 {data.index_version}</Typography.Text>
      </Space>
      {data.heading_path.length > 0 && <Typography.Text strong>{data.heading_path.join(" › ")}</Typography.Text>}
      <Typography.Paragraph style={{ whiteSpace: "pre-wrap" }}>
        {highlight(data.text, data.highlights)}
      </Typography.Paragraph>
      <Typography.Link href={data.source_uri} target="_blank" rel="noreferrer">
        在源系统中打开
      </Typography.Link>
    </Space>
  );
}

// 按高亮区间切分文本；区间按字符位置（与服务端一致的 Unicode 码点）
export function highlight(text: string, ranges: { start: number; end: number }[]) {
  const chars = Array.from(text);
  const sorted = [...ranges].sort((a, b) => a.start - b.start);
  const out: (string | JSX.Element)[] = [];
  let pos = 0;
  sorted.forEach((r, i) => {
    const start = Math.max(r.start, pos);
    const end = Math.min(r.end, chars.length);
    if (end <= start) return;
    out.push(chars.slice(pos, start).join(""));
    out.push(<mark key={i}>{chars.slice(start, end).join("")}</mark>);
    pos = end;
  });
  out.push(chars.slice(pos).join(""));
  return out;
}
