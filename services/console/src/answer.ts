// 一次流式回答的状态：按 SSE 事件累积（事件顺序见 OpenAPI answer_event）。纯函数，便于测试。
import type { AnswerFinal, AnswerMeta, Citation, StreamSentence } from "./api/types";
import type { SSEEvent } from "./api/sse";

export interface AnswerState {
  phase: "streaming" | "done" | "error";
  meta?: AnswerMeta;
  citations: Citation[];
  sentences: StreamSentence[];
  final?: AnswerFinal;
  error?: string;
}

export const initialAnswer: AnswerState = { phase: "streaming", citations: [], sentences: [] };

export function applyEvent(state: AnswerState, ev: SSEEvent): AnswerState {
  let data: unknown;
  try {
    data = JSON.parse(ev.data);
  } catch {
    return { ...state, phase: "error", error: "收到无法解析的事件" };
  }
  switch (ev.event) {
    case "meta":
      return { ...state, meta: data as AnswerMeta };
    case "citation":
      return { ...state, citations: [...state.citations, data as Citation] };
    case "sentence":
      return { ...state, sentences: [...state.sentences, data as StreamSentence] };
    case "final":
      return { ...state, phase: "done", final: data as AnswerFinal };
    case "error": {
      const p = data as { title?: string; detail?: string };
      return { ...state, phase: "error", error: p.detail ? `${p.title}：${p.detail}` : (p.title ?? "回答失败") };
    }
  }
  return state;
}

// 流结束却没有 final：回答未完成（OpenAPI answer_event 的约定）
export function finish(state: AnswerState): AnswerState {
  return state.phase === "streaming" ? { ...state, phase: "error", error: "连接中断，回答没有完成" } : state;
}

// 句子里的 [S#] 引用标记去掉，引用改成句末的角标显示
export function plainText(s: StreamSentence): string {
  return s.text.replace(/\s*\[S\d+\]/g, "").trim();
}

export const statusText: Record<AnswerFinal["status"], string> = {
  answered: "已回答",
  evidence_only: "仅给出证据",
  insufficient_evidence: "证据不足",
  refused: "超出范围，未回答",
};

export const intentText: Record<string, string> = {
  single_hop: "单跳",
  multi_hop: "多跳与比较",
  relation: "关系",
  listing: "枚举与汇总",
  out_of_scope: "超出范围",
  structured: "结构化数据",
};

export const classificationText: Record<string, string> = {
  public: "公开",
  internal: "内部",
  confidential: "机密",
  restricted: "受限",
};

export const confidenceColor = { high: "green", medium: "gold", low: "red" } as const;
export const confidenceText = { high: "高", medium: "中", low: "低" } as const;
