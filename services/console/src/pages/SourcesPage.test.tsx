import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ConsoleContext } from "../context";
import { SourcesPage } from "./SourcesPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

const restricted = {
  source_id: "src_creditdb",
  type: "db",
  owner_dept: "信贷部",
  default_classification: "restricted",
  description: "客户征信库",
  status: "pending",
  created_by: "paul",
  created_at: "2026-10-10T08:00:00Z",
  approved_by: "dana",
  approved_at: "2026-10-10T08:10:00Z",
  approval_basis: "DATA-9",
};

afterEach(() => vi.unstubAllGlobals());

it("受限级数据源属主批过后只差信息安全审批", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(new Response(JSON.stringify({ sources: [restricted] }), { status: 200 })),
      ),
  );
  render(
    <ConsoleContext.Provider value={value}>
      <SourcesPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("客户征信库")).toBeInTheDocument();
  expect(screen.getByText("待信息安全审批")).toBeInTheDocument();
  await userEvent.click(screen.getByRole("radio"));
  expect(await screen.findByRole("button", { name: "信息安全审批" })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "属主审批" })).not.toBeInTheDocument();
});

it("授权申请：待审批的可以审批，没有权限时不显示卡片", async () => {
  const pending = {
    application_id: "gra_000000000000000000000001",
    source_id: "src_creditdb",
    tenant: "credit",
    reason: "信贷问答需要征信库",
    status: "pending",
    applied_by: "owen",
    applied_at: "2026-10-10T08:00:00Z",
  };
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockImplementation((url: string) =>
        Promise.resolve(
          new Response(
            JSON.stringify(url.includes("grant-applications") ? { applications: [pending] } : { sources: [] }),
            { status: 200 },
          ),
        ),
      ),
  );
  const { unmount } = render(
    <ConsoleContext.Provider value={value}>
      <SourcesPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("信贷问答需要征信库")).toBeInTheDocument();
  expect(screen.getByText("待审批")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /^审\s*批$/ })).toBeInTheDocument();
  unmount();

  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation((url: string) =>
      Promise.resolve(
        url.includes("grant-applications")
          ? new Response(JSON.stringify({ type: "forbidden", title: "无权使用该租户或功能", status: 403 }), {
              status: 403,
              headers: { "Content-Type": "application/problem+json" },
            })
          : new Response(JSON.stringify({ sources: [] }), { status: 200 }),
      ),
    ),
  );
  render(
    <ConsoleContext.Provider value={value}>
      <SourcesPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("没有可见的数据源")).toBeInTheDocument();
  await waitFor(() => expect(screen.queryByText("授权申请")).not.toBeInTheDocument());
});
