# orchestrator：问答（单跳与多步检索，Python）。依赖按 uv.lock 安装，只装 orchestrator 与它用到的工作区成员。
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /usr/local/bin/
LABEL org.bambuflow.role=service
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH
WORKDIR /src
# uv 校验锁文件时需要整个工作区的成员清单，所以拷贝全部成员的 pyproject.toml，源码只拷 orchestrator 用到的
COPY pyproject.toml uv.lock ./
COPY contracts/gen/python contracts/gen/python
COPY libs/platform-py libs/platform-py
COPY libs/grounding libs/grounding
COPY libs/vocabulary libs/vocabulary
COPY services/qa/orchestrator services/qa/orchestrator
COPY services/ingestion/processing/pyproject.toml services/ingestion/processing/pyproject.toml
COPY services/evaluation/pyproject.toml services/evaluation/pyproject.toml
COPY tools/e2e/pyproject.toml tools/e2e/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package bambuflow-orchestrator
USER 65532:65532
ENTRYPOINT ["python", "-m", "bambuflow_orchestrator"]
CMD ["serve"]
