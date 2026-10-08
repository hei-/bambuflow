# connectors：runtime 为生产镜像；dev 另含 tools/test-upstream 下的 Wiki 与共享盘样例，只用于开发与 CI。
FROM golang:1.26.8-bookworm AS build
# Go 模块代理：默认官方；访问不了官方代理时构建参数 GOPROXY 改用其他代理（Makefile 的 BF_GOPROXY），校验和照常验证
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local CGO_ENABLED=0 GOWORK=off
WORKDIR /src
COPY contracts/gen/go contracts/gen/go
COPY libs/platform-go libs/platform-go
COPY services/ingestion/connectors services/ingestion/connectors
WORKDIR /src/services/ingestion/connectors
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/connectors ./cmd/connectors

FROM debian:bookworm-slim AS runtime
LABEL org.bambuflow.role=service
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/connectors /usr/local/bin/connectors
USER 65532:65532
ENTRYPOINT ["connectors"]
CMD ["sync-wiki"]

# 干扰页面（P1-T1b）由脚本确定地生成，不进仓库；只进开发镜像
FROM python:3.12-slim-bookworm AS distractors
COPY tools/test-upstream/gen_distractors.py /gen.py
RUN python /gen.py /out

FROM runtime AS dev
COPY tools/test-upstream/wiki /opt/bambuflow/wiki
COPY --from=distractors /out/ /opt/bambuflow/wiki/
COPY tools/test-upstream/fileshare /opt/bambuflow/fileshare
COPY tools/test-upstream/wiki-inject /opt/bambuflow/wiki-inject
COPY tools/test-upstream/logs.yaml /opt/bambuflow/logs.yaml
COPY tools/test-upstream/dbschema.yaml /opt/bambuflow/dbschema.yaml
