import { useCallback, useEffect, useRef, useState } from "react";

// 读接口的通用状态：加载中、结果、错误与重新加载。key 变化（如查询条件、租户）时重新加载；
// load 放在 ref 里，每次渲染换成最新的闭包，不作为依赖
export function useLoad<T>(load: (signal: AbortSignal) => Promise<T>, key: string) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<unknown>();
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  });
  useEffect(() => {
    const ctl = new AbortController();
    setLoading(true);
    setError(undefined);
    loadRef
      .current(ctl.signal)
      .then((d) => setData(d))
      .catch((e: unknown) => {
        if (!(e instanceof DOMException && e.name === "AbortError")) setError(e);
      })
      .finally(() => {
        if (!ctl.signal.aborted) setLoading(false);
      });
    return () => ctl.abort();
  }, [key, tick]);
  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, reload };
}
