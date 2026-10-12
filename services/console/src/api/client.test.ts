import { ApiError, describe as describeError, parseStepUp, request, send } from "./client";

const session = { token: () => "tok", tenant: () => "credit" };

afterEach(() => vi.unstubAllGlobals());

describe("send", () => {
  it("带令牌、租户与幂等键，管理接口不带租户", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    await send(session, "/v1/answers", { body: { q: 1 }, withTenant: true, idempotencyKey: "k1" });
    await send(session, "/v1/admin/audit", { query: { sub: "alice", doc_id: undefined } });
    const [url1, init1] = fetchMock.mock.calls[0];
    expect(url1).toBe("/v1/answers");
    expect(init1.method).toBe("POST");
    expect(init1.headers).toMatchObject({
      Authorization: "Bearer tok",
      "Bambuflow-Tenant": "credit",
      "Idempotency-Key": "k1",
    });
    const [url2, init2] = fetchMock.mock.calls[1];
    expect(url2).toBe("/v1/admin/audit?sub=alice");
    expect(init2.method).toBe("GET");
    expect(init2.headers["Bambuflow-Tenant"]).toBeUndefined();
  });

  it("problem 响应转成 ApiError，按状态给出说明", async () => {
    const body = JSON.stringify({
      type: "self_approval_forbidden",
      title: "不能审批自己发起的操作",
      status: 403,
      trace_id: "t1",
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(body, { status: 403 })));
    const err = await request(session, "/v1/admin/x").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).traceId).toBe("t1");
    expect(describeError(err)).toContain("不能复核自己发起的操作");
    expect(new ApiError(503, "service_unavailable", "x").retryable).toBe(true);
  });

  it("审批要求加强认证时交给 stepUp，并说明要重新认证", async () => {
    const body = JSON.stringify({ type: "insufficient_user_authentication", title: "该操作需要重新认证", status: 401 });
    const header =
      'Bearer error="insufficient_user_authentication", error_description="x", acr_values="2 mfa", max_age=600';
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(body, { status: 401, headers: { "WWW-Authenticate": header } })),
    );
    const stepUp = vi.fn();
    const err = await send({ ...session, stepUp }, "/v1/admin/x/approve", { method: "POST" }).catch((e: unknown) => e);
    expect(stepUp).toHaveBeenCalledWith({ acrValues: "2 mfa", maxAge: 600 });
    expect(describeError(err)).toContain("重新认证");
    // 普通的 401（令牌过期）不触发
    expect(parseStepUp('Bearer error="invalid_token"')).toBeUndefined();
  });

  it("网络错误给出可读的说明", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed to fetch")));
    const err = await send(session, "/v1/x").catch((e: unknown) => e);
    expect((err as ApiError).type).toBe("network_error");
  });
});
