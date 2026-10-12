import { applyEvent, finish, initialAnswer, plainText } from "./answer";
import type { StreamSentence } from "./api/types";

const ev = (event: string, data: unknown) => ({ event, data: JSON.stringify(data) });

describe("applyEvent", () => {
  it("按事件顺序累积，final 后完成", () => {
    let s = applyEvent(initialAnswer, ev("meta", { answer_id: "ans_1", trace_id: "t", conversation_id: "conv_1" }));
    s = applyEvent(s, ev("citation", { ref_id: 1, title: "授信制度" }));
    s = applyEvent(s, ev("sentence", { text: "上限 30 万元 [S1]", ref_ids: [1], block_id: "b1" }));
    expect(s.phase).toBe("streaming");
    s = applyEvent(s, ev("final", { status: "answered", why: {}, answer_kind: "fact" }));
    expect(s.phase).toBe("done");
    expect(s.meta?.answer_id).toBe("ans_1");
    expect(s.citations).toHaveLength(1);
    expect(s.sentences).toHaveLength(1);
  });

  it("error 事件与没有 final 的断开都算失败", () => {
    expect(applyEvent(initialAnswer, ev("error", { title: "服务暂时不可用" })).error).toBe("服务暂时不可用");
    expect(finish(initialAnswer)).toMatchObject({ phase: "error", error: "连接中断，回答没有完成" });
    const done = applyEvent(initialAnswer, ev("final", { status: "refused", why: {}, answer_kind: "fact" }));
    expect(finish(done)).toBe(done);
  });

  it("引用标记从正文里去掉", () => {
    expect(plainText({ text: "上限 30 万元 [S1][S2]。" } as StreamSentence)).toBe("上限 30 万元。");
  });
});
