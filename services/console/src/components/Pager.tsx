import { Button, Space } from "antd";
import { useState } from "react";

// 按 next_page_token 翻页：记下走过的每一页的令牌，能回到上一页；查询条件变了用 reset 回到第一页
export function usePages() {
  const [pages, setPages] = useState<string[]>([""]);
  return {
    token: pages[pages.length - 1] || undefined,
    hasPrev: pages.length > 1,
    prev: () => setPages((p) => p.slice(0, -1)),
    next: (token: string) => setPages((p) => [...p, token]),
    reset: () => setPages([""]),
  };
}

export function Pager({ pages, nextToken }: { pages: ReturnType<typeof usePages>; nextToken?: string }) {
  if (!pages.hasPrev && !nextToken) return null;
  return (
    <Space style={{ marginTop: 8 }}>
      <Button size="small" disabled={!pages.hasPrev} onClick={pages.prev}>
        上一页
      </Button>
      <Button size="small" disabled={!nextToken} onClick={() => nextToken && pages.next(nextToken)}>
        下一页
      </Button>
    </Space>
  );
}
