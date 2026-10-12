# 开发工具镜像：编译、lint、测试、代码生成都在这里跑，本机只需要 git、make 和 docker 客户端。
# 远端 Docker 上下文不能挂载本地目录，所以源码随构建上下文打进镜像（最后一层），缓存放在命名卷里。
# Go 版本与 go.work 一致；uv 版本与 CI 一致。
FROM golang:1.26.8-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /uvx /usr/local/bin/
# console（TypeScript）的 lint、测试与契约类型生成；Node 主版本与 services/console/Dockerfile 一致
COPY --from=node:24-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=node:24-bookworm-slim /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

LABEL org.bambuflow.role=toolbox

# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
# npm 源：默认官方，国内访问慢时由构建参数 NPM_REGISTRY 改用镜像（Makefile 的 BF_NPM_REGISTRY）
ARG NPM_REGISTRY=https://registry.npmjs.org
ENV GOPROXY=${GOPROXY} \
    npm_config_registry=${NPM_REGISTRY} \
    npm_config_update_notifier=false \
    npm_config_fund=false \
    npm_config_audit=false \
    GOTOOLCHAIN=local \
    GOFLAGS=-buildvcs=false \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_LINK_MODE=copy \
    IN_TOOLBOX=1

RUN uv python install 3.12 \
 && git config --system --add safe.directory /src

WORKDIR /src
COPY . .
