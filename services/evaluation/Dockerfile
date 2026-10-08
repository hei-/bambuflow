# evaluation（Python）：runner 以回放模式调用 orchestrator，评测集随镜像打包；serve 为 A/B 分析常驻服务，guard 为护栏检查。
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /usr/local/bin/
LABEL org.bambuflow.role=service
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH
WORKDIR /src
# uv 校验锁文件时需要整个工作区的成员清单，所以拷贝全部成员的 pyproject.toml，源码只拷 evaluation 用到的
COPY pyproject.toml uv.lock ./
COPY contracts/gen/python contracts/gen/python
COPY libs/grounding libs/grounding
COPY libs/platform-py libs/platform-py
COPY services/evaluation services/evaluation
COPY libs/vocabulary/pyproject.toml libs/vocabulary/pyproject.toml
COPY services/qa/orchestrator/pyproject.toml services/qa/orchestrator/pyproject.toml
COPY services/ingestion/processing/pyproject.toml services/ingestion/processing/pyproject.toml
COPY tools/e2e/pyproject.toml tools/e2e/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package bambuflow-evaluation
COPY golden golden
RUN mkdir /out && chown 65532:65532 /out
USER 65532:65532
ENTRYPOINT ["python", "-m", "bambuflow_evaluation"]
