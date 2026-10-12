import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ConsoleContext } from "../context";
import { AskPage } from "./AskPage";

function sse(events: [string, unknown][]): Response {
  const body = events.map(([e, d]) => `event: ${e}\ndata: ${JSON.stringify(d)}\n\n`).join("");
  const stream = new ReadableStream({
    start(c) {
      // 分两段送出，验证跨段拼接
      const enc = new TextEncoder().encode(body);
      c.enqueue(enc.slice(0, 40));
      c.enqueue(enc.slice(40));
      c.close();
    },
  });
  return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

const citation = {
  ref_id: 1,
  chunk_id: "c1",
  doc_id: "d1",
  doc_version: 1,
  title: "个人消费贷款管理办法",
  quote: "额度上限为 30 万元",
  span: { char_start: 0, char_end: 10 },
  source_uri: "https://wiki/1",
  classification: "internal",
  source_type: "wiki",
};

function renderPage() {
  const value = {
    config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
    tenant: "credit",
    session: { token: () => "tok", tenant: () => "credit" },
  };
  return render(
    <ConsoleContext.Provider value={value}>
      <AskPage />
    </ConsoleContext.Provider>,
  );
}

afterEach(() => vi.unstubAllGlobals());

it("流式显示带引用的回答，提交反馈", async () => {
  const fetchMock = vi.fn().mockImplementation((url: string) =>
    Promise.resolve(
      url === "/v1/answers"
        ? sse([
            ["meta", { answer_id: "ans_1", trace_id: "t1", conversation_id: "conv_1" }],
            ["citation", citation],
            [
              "sentence",
              {
                text: "个人消费贷款额度上限为 30 万元 [S1]。",
                start: 0,
                end: 10,
                claim_type: "quote",
                ref_ids: [1],
                support: 0.95,
                block_id: "b1",
                block_type: "paragraph",
              },
            ],
            [
              "final",
              {
                status: "answered",
                why: { index_version: "ix-1", intent: "single_hop" },
                answer_kind: "fact",
                confidence: {
                  score: 0.9,
                  level: "high",
                  weights_version: "w1",
                  coverage: 1,
                  mean_support: 0.95,
                  top_rerank: 0.8,
                  source_diversity: 0.3,
                  conflict: false,
                },
              },
            ],
          ])
        : new Response(JSON.stringify({ rating: "useful" }), { status: 200 }),
    ),
  );
  vi.stubGlobal("fetch", fetchMock);
  const user = userEvent.setup();
  renderPage();
  await user.type(screen.getByPlaceholderText(/输入问题/), "额度上限是多少");
  await user.click(screen.getByRole("button", { name: /提问/ }));

  expect(await screen.findByText("个人消费贷款额度上限为 30 万元。")).toBeInTheDocument();
  expect(screen.getByText("已回答")).toBeInTheDocument();
  expect(screen.getByText("置信度高")).toBeInTheDocument();
  expect(screen.getByText("“额度上限为 30 万元”")).toBeInTheDocument();
  expect(screen.getByText("会话 conv_1")).toBeInTheDocument();

  const [, init] = fetchMock.mock.calls[0];
  expect(JSON.parse(init.body)).toEqual({ question: "额度上限是多少", stream: true });
  expect(init.headers["Bambuflow-Tenant"]).toBe("credit");

  await user.click(screen.getByRole("button", { name: /有\s*用/ }));
  await waitFor(() => expect(screen.getByText("已反馈：有用")).toBeInTheDocument());
  const [url, fb] = fetchMock.mock.calls[1];
  expect(url).toBe("/v1/answers/ans_1/feedback");
  expect(fb.headers["Idempotency-Key"]).toBeTruthy();

  // 删除会话：服务端的会话一并删除，页面回到新会话
  await user.click(screen.getByRole("button", { name: /删除会话/ }));
  await waitFor(() => expect(screen.queryByText("会话 conv_1")).not.toBeInTheDocument());
  const [delURL, del] = fetchMock.mock.calls[2];
  expect(delURL).toBe("/v1/conversations/conv_1");
  expect(del.method).toBe("DELETE");
  expect(del.headers["Bambuflow-Tenant"]).toBe("credit");
});

it("回答失败时显示原因并可重试", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        new Response(
          JSON.stringify({ type: "service_unavailable", title: "服务暂时不可用", status: 503, trace_id: "t9" }),
          { status: 503 },
        ),
      ),
  );
  const user = userEvent.setup();
  renderPage();
  await user.type(screen.getByPlaceholderText(/输入问题/), "问题");
  await user.click(screen.getByRole("button", { name: /提问/ }));
  expect(await screen.findByText("服务暂时不可用")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /重\s*试/ })).toBeInTheDocument();
});
