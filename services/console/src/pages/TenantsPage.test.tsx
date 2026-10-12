import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { TenantsPage } from "./TenantsPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

const active = {
  version_id: "cfv_1",
  object: "tenant",
  scope: "credit",
  number: 3,
  content: "tenant: credit\nrollout: general\n",
  content_hash: "h",
  created_by: "paul",
  created_at: "2026-10-10T08:00:00Z",
  source_ref: "chg_1",
  active: true,
};

afterEach(() => vi.unstubAllGlobals());

const open = {
  chg_id: "chg_" + "a".repeat(24),
  object: "tenant",
  scope: "credit",
  version: { ...active, version_id: "cfv_2", number: 4, active: false },
  reason: "调配额",
  author: "paul",
  created_at: "2026-10-10T09:00:00Z",
  status: "open",
};

function respond(url: string): Response {
  if (url === "/v1/admin/tenants")
    return new Response(JSON.stringify({ tenants: [{ tenant: "credit", rollout: "general", active }] }), {
      status: 200,
    });
  if (url.startsWith("/v1/admin/change-requests?"))
    return new Response(JSON.stringify({ change_requests: [open] }), { status: 200 });
  if (url.startsWith("/v1/admin/releases")) return new Response(JSON.stringify({ releases: [] }), { status: 200 });
  return new Response(JSON.stringify({ ...open, status: "approved", release_id: "rel_1" }), { status: 200 });
}

it("列出待审批的变更单，确认后审批", async () => {
  const fetchMock = vi.fn().mockImplementation((url: string) => Promise.resolve(respond(url)));
  vi.stubGlobal("fetch", fetchMock);
  render(
    <ConsoleContext.Provider value={value}>
      <TenantsPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("#3（cfv_1）")).toBeInTheDocument();
  expect(await screen.findByText("调配额")).toBeInTheDocument();
  expect(fetchMock).toHaveBeenCalledWith("/v1/admin/change-requests?status=open", expect.anything());
  // Ant Design 的表格很大，按文字定位比按角色快得多
  fireEvent.click(screen.getByText("审批通过").closest("button")!);
  fireEvent.click((await screen.findByText("确 定")).closest("button")!);
  await waitFor(() =>
    expect(fetchMock).toHaveBeenCalledWith(
      `/v1/admin/change-requests/${open.chg_id}/approve`,
      expect.objectContaining({ method: "POST" }),
    ),
  );
});

it("灰度中的发布显示范围，可以全量", async () => {
  const rolling = {
    release_id: "rel_" + "b".repeat(24),
    object: "routing",
    scope: "global",
    version: { ...active, object: "routing", scope: "global", version_id: "cfv_9", active: false },
    source: "ci",
    source_ref: "git:3f2a9c1",
    status: "rolling_out",
    reason: "新权重",
    requested_by: "ci-bot",
    requested_at: "2026-10-10T09:00:00Z",
    rollout: {
      tenant: "ops",
      percent: 100,
      started_at: "2026-10-10T10:00:00Z",
      observation: "候选 120 次、对照 900 次",
      observed_at: "2026-10-10T10:05:00Z",
    },
  };
  const fetchMock = vi
    .fn()
    .mockImplementation((url: string) =>
      Promise.resolve(
        url.startsWith("/v1/admin/releases?") || url === "/v1/admin/releases"
          ? new Response(JSON.stringify({ releases: [rolling] }), { status: 200 })
          : url.endsWith("/promote")
            ? new Response(JSON.stringify({ ...rolling, status: "active", rollout: undefined }), { status: 200 })
            : respond(url),
      ),
    );
  vi.stubGlobal("fetch", fetchMock);
  render(
    <ConsoleContext.Provider value={value}>
      <TenantsPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("ops 的 100% 用户")).toBeInTheDocument();
  expect(screen.getByText(/候选 120 次、对照 900 次/)).toBeInTheDocument();
  fireEvent.click(screen.getByText("全 量").closest("button")!);
  fireEvent.click((await screen.findByText("确 定")).closest("button")!);
  await waitFor(() =>
    expect(fetchMock).toHaveBeenCalledWith(`/v1/admin/releases/${rolling.release_id}/promote`, expect.anything()),
  );
});

it("门禁中与门禁未通过的发布显示门禁编号，不能审批", async () => {
  const base = {
    object: "pipeline",
    scope: "ops",
    version: { ...active, object: "pipeline", version_id: "cfv_7", active: false },
    source: "ci",
    source_ref: "git:3f2a9c1",
    reason: "调阈值",
    requested_by: "ci-bot",
    requested_at: "2026-10-10T09:00:00Z",
  };
  const gating = { ...base, release_id: "rel_" + "c".repeat(24), status: "gating", gate_id: "" };
  const failed = {
    ...base,
    release_id: "rel_" + "d".repeat(24),
    status: "gate_failed",
    gate_id: "gate_" + "1".repeat(24),
  };
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockImplementation((url: string) =>
        Promise.resolve(
          url.startsWith("/v1/admin/releases")
            ? new Response(JSON.stringify({ releases: [gating, failed] }), { status: 200 })
            : respond(url),
        ),
      ),
  );
  render(
    <ConsoleContext.Provider value={value}>
      <TenantsPage />
    </ConsoleContext.Provider>,
  );
  const waiting = (await screen.findByText("等待 evaluation 接收门禁")).closest("tr")!;
  const rejected = screen.getByText(`门禁 ${failed.gate_id}`).closest("tr")!;
  expect(within(rejected).getByText("门禁未通过")).toBeInTheDocument();
  // 门禁结束之前（以及门禁不通过的）不能审批
  for (const row of [waiting, rejected]) expect(within(row).queryByText("审批通过")).not.toBeInTheDocument();
});
