# governance：runtime 为生产镜像；dev 另含 tools/test-upstream 下的身份同步测试配置、词表与数据源种子，只用于开发与 CI。
FROM golang:1.26.8-bookworm AS build
# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY services/governance services/governance
WORKDIR /src/services/governance
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/governance ./cmd/governance

FROM debian:bookworm-slim AS runtime
LABEL org.bambuflow.role=service
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/governance /usr/local/bin/governance
# 检索权限策略包的全局基线（信息安全审批）：首次启动时作为初始版本导入（BF_POLICY_SEED_DIRS）
COPY config/policies/global.rego /etc/bambuflow/policies-base/global.rego
USER 65532:65532
ENTRYPOINT ["governance"]
CMD ["identity-sync"]

FROM runtime AS dev
COPY tools/test-upstream/identity-sync.yaml /etc/bambuflow/identity-sync.yaml
COPY tools/test-upstream/vocab.yaml /etc/bambuflow/vocab.yaml
COPY tools/test-upstream/sources.yaml /etc/bambuflow/sources.yaml
COPY tools/test-upstream/tenants /etc/bambuflow/tenants
COPY tools/test-upstream/policies /etc/bambuflow/policies
