# indexing：唯一写索引的服务（生产镜像）。
FROM golang:1.26.8-bookworm AS build
# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY libs/textseg libs/textseg
COPY services/indexing services/indexing
WORKDIR /src/services/indexing
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/indexing ./cmd/indexing

FROM debian:bookworm-slim AS runtime
LABEL org.bambuflow.role=service
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/indexing /usr/local/bin/indexing
USER 65532:65532
ENTRYPOINT ["indexing"]
CMD ["serve"]

