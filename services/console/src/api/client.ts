// gateway 对外接口的调用：同源（nginx 把 /v1 代理到 gateway），带平台令牌与租户请求头。
// 错误统一成 ApiError（RFC 9457 problem 的 type、title、status），页面按 status 与 type 提示。
// 审批类操作要求刚做过加强认证（RFC 9470）：gateway 返回 401 insufficient_user_authentication，
// WWW-Authenticate 里带要求的 acr_values，这里交给 Session.stepUp 引导用户重新认证。

// StepUpChallenge 是 gateway 要求的加强认证
export interface StepUpChallenge {
  acrValues: string;
  maxAge?: number;
}

export function parseStepUp(header: string | null): StepUpChallenge | undefined {
  if (!header || !header.includes('error="insufficient_user_authentication"')) return undefined;
  const acr = /acr_values="([^"]*)"/.exec(header)?.[1] ?? "";
  const maxAge = /max_age=(\d+)/.exec(header)?.[1];
  return { acrValues: acr, maxAge: maxAge === undefined ? undefined : Number(maxAge) };
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly type: string,
    readonly title: string,
    readonly detail?: string,
    readonly traceId?: string,
    readonly stepUp?: StepUpChallenge,
  ) {
    super(detail ? `${title}：${detail}` : title);
  }

  // 下游暂不可用、限流：可以稍后重试
  get retryable(): boolean {
    return this.status === 429 || this.status === 503 || this.status === 504;
  }
}

export interface Session {
  token: () => string | undefined;
  tenant: () => string | undefined;
  // 收到加强认证要求时调用（询问用户后跳转重新登录）；没有时只报错
  stepUp?: (c: StepUpChallenge) => void;
}

export interface RequestOptions {
  method?: "GET" | "POST" | "DELETE";
  query?: Record<string, string | number | undefined>;
  body?: unknown;
  // 管理写操作与反馈要求幂等键（gateway 详设 4.3）；同一次用户操作重试时沿用同一个键
  idempotencyKey?: string;
  // 管理接口不带租户（平台级）；数据接口带当前选择的租户
  withTenant?: boolean;
  accept?: string;
  signal?: AbortSignal;
}

export function newIdempotencyKey(): string {
  return crypto.randomUUID();
}

export function buildURL(path: string, query?: RequestOptions["query"]): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== "") params.set(k, String(v));
  }
  const qs = params.toString();
  return qs ? `${path}?${qs}` : path;
}

export async function send(session: Session, path: string, opts: RequestOptions = {}): Promise<Response> {
  const headers: Record<string, string> = { Accept: opts.accept ?? "application/json" };
  const token = session.token();
  if (token) headers.Authorization = `Bearer ${token}`;
  const tenant = session.tenant();
  if (opts.withTenant && tenant) headers["Bambuflow-Tenant"] = tenant;
  if (opts.idempotencyKey) headers["Idempotency-Key"] = opts.idempotencyKey;
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  let resp: Response;
  try {
    resp = await fetch(buildURL(path, opts.query), {
      method: opts.method ?? (opts.body === undefined ? "GET" : "POST"),
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      signal: opts.signal,
    });
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") throw e;
    throw new ApiError(0, "network_error", "连不上服务", "请检查网络或稍后重试");
  }
  if (!resp.ok) {
    const err = await problemOf(resp);
    if (err.stepUp) session.stepUp?.(err.stepUp);
    throw err;
  }
  return resp;
}

export async function request<T>(session: Session, path: string, opts: RequestOptions = {}): Promise<T> {
  const resp = await send(session, path, opts);
  return (await resp.json()) as T;
}

export async function problemOf(resp: Response): Promise<ApiError> {
  const stepUp = resp.status === 401 ? parseStepUp(resp.headers.get("WWW-Authenticate")) : undefined;
  try {
    const p = (await resp.json()) as { type?: string; title?: string; detail?: string; trace_id?: string };
    return new ApiError(
      resp.status,
      p.type ?? "unknown",
      p.title ?? `请求失败（${resp.status}）`,
      p.detail,
      p.trace_id,
      stepUp,
    );
  } catch {
    return new ApiError(resp.status, "unknown", `请求失败（${resp.status}）`, undefined, undefined, stepUp);
  }
}

// 面向用户的错误说明：未登录、无权、对象状态冲突单独说明，其余用服务端的标题
export function describe(err: unknown): string {
  if (!(err instanceof ApiError)) return err instanceof Error ? err.message : String(err);
  switch (err.status) {
    case 401:
      return err.stepUp ? "审批操作需要重新认证，认证后请再提交一次" : "登录已过期，请重新登录";
    case 403:
      return err.type === "self_approval_forbidden"
        ? "不能复核自己发起的操作，请换一位同事复核"
        : `无权执行：${err.title}`;
    case 404:
      return "对象不存在，或你没有查看它的权限";
  }
  return err.message;
}
