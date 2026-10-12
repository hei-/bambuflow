import { SSEParser } from "./sse";

describe("SSEParser", () => {
  it("事件被切在任意位置也能拼回来，忽略心跳注释", () => {
    const p = new SSEParser();
    expect(p.push('event: meta\ndata: {"a"')).toEqual([]);
    expect(p.push(":1}\n\n: keep-alive\n\nevent: sentence\r\ndata: x\r\n\r\n")).toEqual([
      { event: "meta", data: '{"a":1}' },
      { event: "sentence", data: "x" },
    ]);
  });

  it("多行 data 用换行连接，没有 event 字段时是 message", () => {
    const p = new SSEParser();
    expect(p.push("data: a\ndata: b\n\n")).toEqual([{ event: "message", data: "a\nb" }]);
  });
});
