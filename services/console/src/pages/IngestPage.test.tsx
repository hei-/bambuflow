import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { IngestPage } from "./IngestPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

const dead = {
  doc_id: "doc-1",
  source_id: "src_wikidemo",
  event_id: "evt-1",
  stage: "parse",
  error: { code: "bad_document", message: "解析失败" },
  pipeline_version: "p1",
  attempts: 5,
  replays: 0,
  quarantined: false,
  first_failed_at: "2026-10-10T08:00:00Z",
  last_failed_at: "2026-10-10T08:05:00Z",
};

afterEach(() => vi.unstubAllGlobals());

it("没选文档也没筛选时不能重放；选中后只重放选中的文档", async () => {
  const fetchMock = vi.fn().mockImplementation((url: string) =>
    Promise.resolve(
      url.startsWith("/v1/admin/dead-letters")
        ? new Response(JSON.stringify({ dead_letters: [dead] }), { status: 200 })
        : new Response(JSON.stringify({ ingest_batch_id: "ing_1", replayed: 1, skipped_quarantined: 0 }), {
            status: 200,
          }),
    ),
  );
  vi.stubGlobal("fetch", fetchMock);
  render(
    <ConsoleContext.Provider value={value}>
      <IngestPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("bad_document：解析失败")).toBeInTheDocument();
  // Ant Design 的表格与弹窗很大，按文字定位比按角色快得多
  expect(screen.getByText("按筛选条件重放").closest("button")).toBeDisabled();
  fireEvent.click(screen.getByText("doc-1").closest("tr")!.querySelector("input[type=checkbox]")!);
  fireEvent.click((await screen.findByText("重放选中的 1 份")).closest("button")!);
  fireEvent.change(await screen.findByLabelText("原因"), { target: { value: "修了解析器" } });
  fireEvent.click(screen.getByText("提 交").closest("button")!);
  await waitFor(() =>
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/admin/replays",
      expect.objectContaining({ body: JSON.stringify({ doc_ids: ["doc-1"], reason: "修了解析器" }) }),
    ),
  );
});
