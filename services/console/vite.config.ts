import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@contracts": fileURLToPath(new URL("../../contracts/gen/ts", import.meta.url)) },
  },
  // 开发时经 vite 代理到 gateway（make tunnel 把 gateway 转到本机 27088），与生产镜像里 nginx 的同源代理一致
  server: { proxy: { "/v1": "http://localhost:27088" } },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test-setup.ts"],
    css: false,
    // Ant Design 的组件在 jsdom 里渲染较慢，组件测试给足时间
    testTimeout: 20000,
  },
});
