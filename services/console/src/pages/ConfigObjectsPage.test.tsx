import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { ConfigObjectsPage } from "./ConfigObjectsPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

const active = {
  version_id: "cfv_" + "1".repeat(24),
  object: "policy",
  scope: "tenant:ops",
  number: 2,
  content: 'deny_sources := {"src_evalcredit"}\n',
  content_hash: "h",
  created_by: "ci-bot",
  created_at: "2026-10-10T08:00:00Z",
  source_ref: "git:3f2a9c1",
  active: true,
  widened: ["租户 ops、密级 1：新增可检索数据源 mail"],
};

afterEach(() => vi.unstubAllGlobals());

it("读取生效版本，修改后写成合并请求", async () => {
  const fetchMock = vi.fn().mockImplementation((url: string) =>
    Promise.resolve(
      url.startsWith("/v1/admin/config-versions?")
        ? new Response(JSON.stringify({ versions: [active] }), { status: 200 })
        : new Response(
            JSON.stringify({
              chg_id: "chg_" + "a".repeat(24),
              status: "merge_request",
              merge_request_url: "https://git.corp/config/-/merge_requests/7",
            }),
            { status: 201 },
          ),
    ),
  );
  vi.stubGlobal("fetch", fetchMock);
  render(
    <ConsoleContext.Provider value={value}>
      <ConfigObjectsPage />
    </ConsoleContext.Provider>,
  );
  fireEvent.mouseDown(screen.getByRole("combobox"));
  fireEvent.click(await screen.findByText("策略包的一层"));
  fireEvent.change(screen.getByPlaceholderText(/global、tenant/), { target: { value: "tenant:ops" } });
  fireEvent.click(screen.getByText("读取生效版本").closest("button")!);
  expect(await screen.findByText(/生效版本 #2/)).toBeInTheDocument();
  expect(screen.getByText("租户 ops、密级 1：新增可检索数据源 mail")).toBeInTheDocument();
  expect(fetchMock.mock.calls[0][0]).toBe("/v1/admin/config-versions?object=policy&scope=tenant%3Aops");
  // 内容没改时不能提交
  const submit = screen.getByText("提交合并请求").closest("button")!;
  expect(submit).toBeDisabled();
  fireEvent.change(screen.getByLabelText("内容"), { target: { value: "deny_sources := set()\n" } });
  fireEvent.change(screen.getByLabelText("修改原因"), { target: { value: "放开评测数据源" } });
  fireEvent.click(submit);
  expect(await screen.findByText("https://git.corp/config/-/merge_requests/7")).toBeInTheDocument();
  const [url, init] = fetchMock.mock.calls[1];
  expect(url).toBe("/v1/admin/change-requests");
  await waitFor(() =>
    expect(JSON.parse(init.body)).toMatchObject({
      object: "policy",
      scope: "tenant:ops",
      base_version_id: active.version_id,
      content: "deny_sources := set()\n",
    }),
  );
  expect(init.headers["Idempotency-Key"]).toBeTruthy();
});
