// 运行时配置：由镜像启动时按环境变量写出 /config.json（deploy 不同环境不用重新构建）。
export interface Named {
  id: string;
  name: string;
}

export interface ConsoleConfig {
  authority: string;
  clientId: string;
  // 登录时直接跳到公司 IdP，省去平台登录页上的选择
  idpHint?: string;
  tenants: Named[];
}

export async function loadConfig(): Promise<ConsoleConfig> {
  const resp = await fetch("/config.json", { cache: "no-store" });
  if (!resp.ok) throw new Error(`读取 /config.json 失败：${resp.status}`);
  return (await resp.json()) as ConsoleConfig;
}
