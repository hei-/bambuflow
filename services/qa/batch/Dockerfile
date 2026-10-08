# qa/batch：异步批量问答（技术设计文档 13.5）。
FROM golang:1.26.8-bookworm AS build
# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY services/qa/batch services/qa/batch
WORKDIR /src/services/qa/batch
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/batch ./cmd/batch

FROM debian:bookworm-slim AS runtime
LABEL org.bambuflow.role=service
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/batch /usr/local/bin/batch
USER 65532:65532
ENTRYPOINT ["batch"]
CMD ["serve"]
