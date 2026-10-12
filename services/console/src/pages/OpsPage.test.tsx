import { render, screen } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { OpsPage } from "./OpsPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

afterEach(() => vi.unstubAllGlobals());

it("列出没跟上的实例数；删除凭证没权限时单独提示", async () => {
  const sync = {
    services: [
      {
        service: "gateway",
        current_version: "tenants:abc",
        instances: [
          {
            instance_id: "gw-1",
            snapshot_version: "tenants:abc",
            loaded: true,
            acked_at: "2026-10-10T08:00:00Z",
            in_sync: true,
          },
          {
            instance_id: "gw-2",
            snapshot_version: "tenants:old",
            loaded: true,
            acked_at: "2026-10-10T08:00:00Z",
            in_sync: false,
          },
        ],
      },
    ],
  };
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockImplementation((url: string) =>
        Promise.resolve(
          url === "/v1/admin/snapshot-sync"
            ? new Response(JSON.stringify(sync), { status: 200 })
            : new Response("{}", { status: 500 }),
        ),
      ),
  );
  render(
    <ConsoleContext.Provider value={value}>
      <OpsPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("tenants:abc")).toBeInTheDocument();
  const row = screen.getByText("gateway").closest("tr");
  expect(row?.textContent).toContain("2");
  expect(row?.querySelector(".ant-tag-red")?.textContent).toBe("1");
  expect(screen.getByText("按文档或数据源查询")).toBeInTheDocument();
});
