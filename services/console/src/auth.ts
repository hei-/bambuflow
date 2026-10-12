import { UserManager, WebStorageStateStore, type User } from "oidc-client-ts";
import type { ConsoleConfig } from "./config";

// 授权码 + PKCE（公开客户端，没有密钥）。令牌只放在 sessionStorage：关掉标签页即失效
export function createUserManager(cfg: ConsoleConfig): UserManager {
  return new UserManager({
    authority: cfg.authority,
    client_id: cfg.clientId,
    redirect_uri: `${window.location.origin}/callback`,
    // 退出后也回到 /callback（Keycloak 只接受登记过的回调地址），回调页看到没有授权码就回登录页
    post_logout_redirect_uri: `${window.location.origin}/callback`,
    response_type: "code",
    scope: "openid",
    extraQueryParams: cfg.idpHint ? { kc_idp_hint: cfg.idpHint } : {},
    userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    automaticSilentRenew: false,
  });
}

export function displayName(user: User): string {
  const p = user.profile;
  return (p.name as string | undefined) ?? (p.preferred_username as string | undefined) ?? p.sub;
}
