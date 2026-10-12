import { render, screen } from "@testing-library/react";
import { ConsoleContext } from "../context";
import { VocabularyPage } from "./VocabularyPage";

const value = {
  config: { authority: "", clientId: "console", tenants: [{ id: "credit", name: "信贷" }] },
  tenant: "credit",
  session: { token: () => "tok", tenant: () => "credit" },
};

afterEach(() => vi.unstubAllGlobals());

it("停用的词条显示替代编码，不能再修改", async () => {
  const terms = [
    { vocabulary: "doc_type", code: "REG", name: "制度", synonyms: ["办法"], status: "active" },
    { vocabulary: "doc_type", code: "RULE", name: "规定", synonyms: [], status: "deprecated", replaced_by: "REG" },
  ];
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response(JSON.stringify({ terms, snapshot_version: "vocab:1" }), { status: 200 })),
  );
  render(
    <ConsoleContext.Provider value={value}>
      <VocabularyPage />
    </ConsoleContext.Provider>,
  );
  expect(await screen.findByText("已停用 → REG")).toBeInTheDocument();
  expect(screen.getByText("快照版本：vocab:1")).toBeInTheDocument();
  expect(screen.getAllByRole("button", { name: "修 改" })).toHaveLength(1);
});
