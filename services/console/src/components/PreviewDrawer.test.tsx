import { render } from "@testing-library/react";
import { highlight } from "./PreviewDrawer";

it("按字符位置高亮，含多字节字符", () => {
  const { container } = render(<p>{highlight("额度上限为 30 万元。", [{ start: 2, end: 4 }])}</p>);
  expect(container.querySelector("mark")?.textContent).toBe("上限");
  expect(container.textContent).toBe("额度上限为 30 万元。");
});
