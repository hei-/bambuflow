# 测试工具镜像（devseed 与 mock-embedder、mock-nli、mock-llm 等模拟服务），只用于开发与 CI，生产 chart 不包含它。
FROM golang:1.26.8-bookworm AS build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY tools/devtools tools/devtools
WORKDIR /src/tools/devtools
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM debian:bookworm-slim
LABEL org.bambuflow.role=test-upstream
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
