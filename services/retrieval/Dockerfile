# retrieval：runtime 为生产镜像。检索权限策略包与租户可访问的数据源都由 governance 经配置快照下发，镜像里不带；
# dev 与 runtime 相同（保留这个目标供 compose 统一使用）。
FROM golang:1.26.8-bookworm AS build
# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY libs/textseg libs/textseg
COPY services/retrieval services/retrieval
WORKDIR /src/services/retrieval
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/retrieval ./cmd/retrieval

FROM debian:bookworm-slim AS runtime
LABEL org.bambuflow.role=service
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/retrieval /usr/local/bin/retrieval
USER 65532:65532
ENTRYPOINT ["retrieval"]
CMD ["serve"]

FROM runtime AS dev
