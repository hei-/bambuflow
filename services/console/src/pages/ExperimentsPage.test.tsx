import { render, screen, within } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { ExperimentsPage } from "./ExperimentsPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

const version = "cfv_" + "2".repeat(24);

afterEach(() => vi.unstubAllGlobals());

it("列表显示实验组替换的阈值与配置版本", async () => {
  const experiment = {
    exp_id: "exp_" + "a".repeat(24),
    tenant: "credit",
    hypothesis: "换回答模板后有用反馈率上升",
    primary_metric: "useful_rate",
    baseline_rate: 0.6,
    min_detectable_effect: 0.05,
    sample_size_per_arm: 1470,
    traffic_percent: 5,
    overrides: { min_coverage: 0.7, config_versions: [version] },
    status: "running",
    created_by: "paul",
    created_at: "2026-10-11T08:00:00Z",
  };
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response(JSON.stringify({ experiments: [experiment] }), { status: 200 })),
  );
  render(
    <ConsoleContext.Provider value={value}>
      <ExperimentsPage />
    </ConsoleContext.Provider>,
  );
  const row = (await screen.findByText(experiment.exp_id)).closest("tr")!;
  expect(within(row).getByText(`覆盖率门槛 0.7；配置 ${version}`)).toBeInTheDocument();
  expect(within(row).getByText("推进到 20%")).toBeInTheDocument();
});
