# 管理控制台（技术设计文档 12.1：TypeScript、React + Ant Design、Vite）。构建上下文是仓库根目录：
# 接口类型来自 contracts/gen/ts（make contracts 生成）。运行时是 nginx：静态文件 + 把 /v1 同源代理到 gateway。
FROM node:24-bookworm-slim AS build
ARG NPM_REGISTRY=https://registry.npmjs.org
ENV npm_config_registry=${NPM_REGISTRY} npm_config_update_notifier=false npm_config_fund=false npm_config_audit=false
WORKDIR /src
COPY services/console/package.json services/console/package-lock.json services/console/
RUN cd services/console && npm ci --loglevel=error
COPY contracts/gen/ts contracts/gen/ts
COPY services/console services/console
RUN cd services/console && npm run build

FROM nginx:1.29-alpine AS runtime
LABEL org.bambuflow.role=console
COPY --from=build /src/services/console/dist /usr/share/nginx/html
COPY services/console/deploy/default.conf.template /etc/nginx/templates/default.conf.template
COPY services/console/deploy/40-console-config.sh /docker-entrypoint.d/40-console-config.sh
# gateway 的地址（同源代理的上游）；运行时配置见 40-console-config.sh
ENV BF_GATEWAY_URL=http://gateway:8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
