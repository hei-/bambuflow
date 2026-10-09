# gateway：runtime 为生产镜像；dev 另含 tools/test-upstream 下的测试租户配置，只用于开发与 CI。
FROM golang:1.26.8-bookworm AS build
# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY services/gateway services/gateway
WORKDIR /src/services/gateway
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

FROM debian:bookworm-slim AS runtime
LABEL org.bambuflow.role=service
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/gateway /usr/local/bin/gateway
# 入口审计的本地持久队列；命名卷挂载时沿用这里的属主
RUN mkdir -p /var/lib/gateway/audit && chown -R 65532:65532 /var/lib/gateway
USER 65532:65532
ENTRYPOINT ["gateway"]
CMD ["serve"]

FROM runtime AS dev
COPY tools/test-upstream/admin-roles.yaml /etc/bambuflow/admin-roles.yaml
