import { render, screen, within } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { AuditPage } from "./AuditPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

afterEach(() => vi.unstubAllGlobals());

it("线上访问与变更审计都列出来，变更审计显示动作与对象", async () => {
  const empty = { candidate_chunk_ids: [], context_chunk_ids: [], shown_chunk_ids: [], listed_doc_ids: [] };
  const records = [
    {
      ...empty,
      kind: "access",
      source: "gateway",
      time: "2026-10-11T08:00:00Z",
      trace_id: "t1",
      sub: "alice",
      endpoint: "answers",
      status: 200,
      latency_ms: 812,
    },
    {
      ...empty,
      kind: "change",
      source: "indexing",
      time: "2026-10-11T08:01:00Z",
      trace_id: "",
      sub: "service:indexing",
      endpoint: "",
      status: 0,
      latency_ms: 0,
      action: "document.purge",
      object_id: "loan",
    },
  ];
  const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ records }), { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  render(
    <ConsoleContext.Provider value={value}>
      <AuditPage />
    </ConsoleContext.Provider>,
  );
  const change = (await screen.findByText("document.purge loan")).closest("tr")!;
  expect(within(change).getByText("变更·indexing")).toBeInTheDocument();
  const access = screen.getByText("answers").closest("tr")!;
  expect(within(access).getByText("线上访问·gateway")).toBeInTheDocument();
  expect(within(access).getByText("812 ms")).toBeInTheDocument();
  // 不选类别时不带 kind：由 governance 取线上访问与变更
  expect(fetchMock.mock.calls[0][0]).not.toContain("kind=");
});
