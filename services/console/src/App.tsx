import {
  AppstoreOutlined,
  AuditOutlined,
  BookOutlined,
  CloudSyncOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  ExperimentOutlined,
  FundOutlined,
  InboxOutlined,
  LogoutOutlined,
  MessageOutlined,
  SettingOutlined,
  BranchesOutlined,
  StopOutlined,
  TagsOutlined,
} from "@ant-design/icons";
import { Alert, Button, Card, Layout, Menu, Modal, Select, Space, Spin, Typography } from "antd";
import type { User, UserManager } from "oidc-client-ts";
import type { StepUpChallenge } from "./api/client";
import { useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { displayName } from "./auth";
import type { ConsoleConfig } from "./config";
import { ConsoleContext } from "./context";
import { AskPage } from "./pages/AskPage";
import { AuditPage } from "./pages/AuditPage";
import { DenialsPage } from "./pages/DenialsPage";
import { EvalGatesPage } from "./pages/EvalGatesPage";
import { ExperimentsPage } from "./pages/ExperimentsPage";
import { GoldenPage } from "./pages/GoldenPage";
import { IndexVersionsPage } from "./pages/IndexVersionsPage";
import { IngestPage } from "./pages/IngestPage";
import { OpsPage } from "./pages/OpsPage";
import { PluginsPage } from "./pages/PluginsPage";
import { SourcesPage } from "./pages/SourcesPage";
import { TenantsPage } from "./pages/TenantsPage";
import { ConfigObjectsPage } from "./pages/ConfigObjectsPage";
import { VocabularyPage } from "./pages/VocabularyPage";

const menu = [
  { key: "/", icon: <MessageOutlined />, label: "问答" },
  { key: "/admin/sources", icon: <CloudSyncOutlined />, label: "数据源" },
  { key: "/admin/ingest", icon: <InboxOutlined />, label: "入库运维" },
  { key: "/admin/index-versions", icon: <DatabaseOutlined />, label: "索引版本" },
  { key: "/admin/eval-gates", icon: <ExperimentOutlined />, label: "评测门禁" },
  { key: "/admin/golden-sets", icon: <BookOutlined />, label: "评测集" },
  { key: "/admin/tenants", icon: <SettingOutlined />, label: "租户配置" },
  { key: "/admin/config-objects", icon: <BranchesOutlined />, label: "配置对象" },
  { key: "/admin/experiments", icon: <FundOutlined />, label: "A/B 实验" },
  { key: "/admin/vocabularies", icon: <TagsOutlined />, label: "受控词表" },
  { key: "/admin/plugins", icon: <AppstoreOutlined />, label: "插件" },
  { key: "/admin/emergency-denials", icon: <StopOutlined />, label: "紧急拒绝" },
  { key: "/admin/audit", icon: <AuditOutlined />, label: "审计" },
  { key: "/admin/ops", icon: <DashboardOutlined />, label: "运维状态" },
];

const TENANT_KEY = "bambuflow.tenant";

export function App({ config, users }: { config: ConsoleConfig; users: UserManager }) {
  const [user, setUser] = useState<User | null>();
  const [authError, setAuthError] = useState<string>();
  const location = useLocation();

  useEffect(() => {
    if (window.location.pathname === "/callback") return;
    users
      .getUser()
      .then((u) => setUser(u && !u.expired ? u : null))
      .catch((e: unknown) => {
        setAuthError(e instanceof Error ? e.message : String(e));
        setUser(null);
      });
  }, [users]);

  if (location.pathname === "/callback") return <Callback users={users} onUser={setUser} onError={setAuthError} />;
  if (user === undefined) return <Spin fullscreen />;
  if (!user) return <Login users={users} error={authError} />;
  return <Shell config={config} users={users} user={user} />;
}

// 授权码只能换一次令牌；开发模式下 StrictMode 会把 effect 执行两次，所以同一页面只换一次
let callbackOnce: Promise<User> | undefined;

// 登录回调：用授权码换令牌后回到发起登录的页面（重新认证时带在 state 里），否则回首页
function Callback({
  users,
  onUser,
  onError,
}: {
  users: UserManager;
  onUser: (u: User | null) => void;
  onError: (e: string) => void;
}) {
  const navigate = useNavigate();
  useEffect(() => {
    if (!new URLSearchParams(window.location.search).has("code")) {
      // 退出登录后的回跳
      onUser(null);
      navigate("/", { replace: true });
      return;
    }
    callbackOnce ??= users.signinRedirectCallback();
    let returnTo = "/";
    callbackOnce
      .then((u) => {
        returnTo = safeReturnTo(u.state);
        onUser(u);
      })
      .catch((e: unknown) => {
        onError(e instanceof Error ? e.message : String(e));
        onUser(null);
      })
      .finally(() => navigate(returnTo, { replace: true }));
  }, [users, navigate, onUser, onError]);
  return <Spin fullscreen tip="正在登录…" />;
}

// 只接受站内路径，避免把回跳地址变成开放重定向
function safeReturnTo(state: unknown): string {
  const to = (state as { returnTo?: unknown } | undefined)?.returnTo;
  return typeof to === "string" && to.startsWith("/") && !to.startsWith("//") ? to : "/";
}

// 审批类操作要求加强认证（RFC 9470）：确认后强制重新登录（prompt=login，SSO 会话不算），按要求的 acr 认证，
// 回到当前页面；表单内容不保留，所以提示用户回来后再提交一次
function askStepUp(users: UserManager, c: StepUpChallenge, returnTo: string) {
  Modal.confirm({
    title: "需要重新认证",
    content: "审批类操作要求刚刚完成加强认证。确认后跳转到登录页，认证后回到本页，请再提交一次。",
    okText: "去认证",
    cancelText: "取消",
    onOk: () =>
      users.signinRedirect({
        prompt: "login",
        acr_values: c.acrValues || undefined,
        state: { returnTo },
      }),
  });
}

function Login({ users, error }: { users: UserManager; error?: string }) {
  const [busy, setBusy] = useState(false);
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        minHeight: "100vh",
        background: "#f5f5f5",
      }}
    >
      <Card style={{ width: 400 }}>
        <Space direction="vertical" size="large" style={{ width: "100%" }}>
          <Typography.Title level={3} style={{ margin: 0 }}>
            bambuflow 控制台
          </Typography.Title>
          <Typography.Text type="secondary">企业知识问答与运维管理。使用公司账号登录，按你的权限检索。</Typography.Text>
          {error && <Alert type="error" showIcon message={`登录失败：${error}`} />}
          <Button
            type="primary"
            block
            loading={busy}
            onClick={() => {
              setBusy(true);
              users.signinRedirect().catch(() => setBusy(false));
            }}
          >
            使用公司账号登录
          </Button>
        </Space>
      </Card>
    </div>
  );
}

function readTenant(config: ConsoleConfig): string {
  try {
    const saved = window.localStorage.getItem(TENANT_KEY);
    if (saved && config.tenants.some((t) => t.id === saved)) return saved;
  } catch {
    // 浏览器禁用了本地存储：用默认租户
  }
  return config.tenants[0]?.id ?? "";
}

function Shell({ config, users, user }: { config: ConsoleConfig; users: UserManager; user: User }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [tenant, setTenant] = useState(() => readTenant(config));
  const value = useMemo(
    () => ({
      config,
      tenant,
      session: {
        token: () => user.access_token,
        tenant: () => tenant,
        stepUp: (c: StepUpChallenge) => askStepUp(users, c, window.location.pathname),
      },
    }),
    [config, tenant, user, users],
  );
  const selected = menu.find((m) => m.key !== "/" && location.pathname.startsWith(m.key))?.key ?? "/";
  return (
    <ConsoleContext.Provider value={value}>
      <Layout style={{ minHeight: "100vh" }}>
        <Layout.Sider theme="light" width={200}>
          <Typography.Title level={4} style={{ padding: "16px 24px", margin: 0 }}>
            bambuflow
          </Typography.Title>
          <Menu mode="inline" selectedKeys={[selected]} items={menu} onClick={(e) => navigate(e.key)} />
        </Layout.Sider>
        <Layout>
          <Layout.Header
            style={{ background: "#fff", display: "flex", justifyContent: "flex-end", alignItems: "center", gap: 16 }}
          >
            <Space>
              <Typography.Text type="secondary">租户</Typography.Text>
              <Select
                style={{ width: 160 }}
                value={tenant}
                onChange={(t) => {
                  setTenant(t);
                  try {
                    window.localStorage.setItem(TENANT_KEY, t);
                  } catch {
                    // 存不下就只在本次会话里生效
                  }
                }}
                options={config.tenants.map((t) => ({ value: t.id, label: t.name }))}
              />
            </Space>
            <Typography.Text>{displayName(user)}</Typography.Text>
            <Button icon={<LogoutOutlined />} onClick={() => void users.signoutRedirect()}>
              退出
            </Button>
          </Layout.Header>
          <Layout.Content style={{ padding: 24 }}>
            <Routes>
              {/* 换租户时问答页重新开始：会话属于租户 */}
              <Route path="/" element={<AskPage key={tenant} />} />
              <Route path="/admin/sources" element={<SourcesPage />} />
              <Route path="/admin/ingest" element={<IngestPage />} />
              <Route path="/admin/index-versions" element={<IndexVersionsPage />} />
              <Route path="/admin/eval-gates" element={<EvalGatesPage />} />
              {/* 换租户时评测集重新加载：评测集按租户 */}
              <Route path="/admin/golden-sets" element={<GoldenPage key={tenant} />} />
              <Route path="/admin/tenants" element={<TenantsPage />} />
              <Route path="/admin/config-objects" element={<ConfigObjectsPage />} />
              <Route path="/admin/experiments" element={<ExperimentsPage />} />
              <Route path="/admin/vocabularies" element={<VocabularyPage />} />
              <Route path="/admin/plugins" element={<PluginsPage />} />
              <Route path="/admin/emergency-denials" element={<DenialsPage />} />
              <Route path="/admin/audit" element={<AuditPage />} />
              <Route path="/admin/ops" element={<OpsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </Layout.Content>
        </Layout>
      </Layout>
    </ConsoleContext.Provider>
  );
}
