import { ConfigProvider, Result } from "antd";
import zhCN from "antd/locale/zh_CN";
import dayjs from "dayjs";
import "dayjs/locale/zh-cn";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App } from "./App";
import { createUserManager } from "./auth";
import { loadConfig } from "./config";

dayjs.locale("zh-cn");

const root = createRoot(document.getElementById("root")!);

loadConfig()
  .then((config) => {
    const users = createUserManager(config);
    root.render(
      <StrictMode>
        <ConfigProvider locale={zhCN}>
          <BrowserRouter>
            <App config={config} users={users} />
          </BrowserRouter>
        </ConfigProvider>
      </StrictMode>,
    );
  })
  .catch((e: unknown) => {
    root.render(
      <Result status="error" title="控制台配置加载失败" subTitle={e instanceof Error ? e.message : String(e)} />,
    );
  });
