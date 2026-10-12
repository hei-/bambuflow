// SSE（text/event-stream）解析：POST 请求不能用 EventSource，按规范自己切分事件。
// 只处理 event 与 data 字段；以冒号开头的行是心跳注释，忽略。

export interface SSEEvent {
  event: string;
  data: string;
}

export class SSEParser {
  private buf = "";

  // 喂入一段文本，返回其中已经完整的事件；不完整的留到下一段
  push(chunk: string): SSEEvent[] {
    this.buf += chunk.replace(/\r\n?/g, "\n");
    const out: SSEEvent[] = [];
    let idx: number;
    while ((idx = this.buf.indexOf("\n\n")) >= 0) {
      const block = this.buf.slice(0, idx);
      this.buf = this.buf.slice(idx + 2);
      const ev = parseBlock(block);
      if (ev) out.push(ev);
    }
    return out;
  }
}

function parseBlock(block: string): SSEEvent | null {
  let event = "message";
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line === "" || line.startsWith(":")) continue;
    const i = line.indexOf(":");
    const field = i < 0 ? line : line.slice(0, i);
    let value = i < 0 ? "" : line.slice(i + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") event = value;
    else if (field === "data") data.push(value);
  }
  return data.length ? { event, data: data.join("\n") } : null;
}

// 逐段读响应体并回调每个事件，直到流结束或 signal 取消
export async function readSSE(resp: Response, onEvent: (e: SSEEvent) => void): Promise<void> {
  if (!resp.body) throw new Error("响应没有正文");
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  const parser = new SSEParser();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    for (const e of parser.push(decoder.decode(value, { stream: true }))) onEvent(e);
  }
  for (const e of parser.push(decoder.decode() + "\n\n")) onEvent(e);
}
