# 开发用模型服务（CPU 推理，KServe v2 接口）：只用于开发与评测，生产用 Triton。
# 依赖按 requirements.txt 的 sha256 安装（--require-hashes）；权重在启动时下载到 /models 并校验 sha256。
FROM python:3.12-slim-bookworm
LABEL org.bambuflow.role=test-upstream
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
# requirements.txt 为 CPU 版 torch；GPU 主机用 requirements-gpu.txt（CUDA 12.6 版，BF_MODELS_GPU=1 时由 compose 叠加文件指定）
ARG REQUIREMENTS=requirements.txt
ARG TORCH_INDEX=https://download.pytorch.org/whl/cpu
# 默认官方 PyPI；访问不了时由构建参数换成镜像（.env.local 的 BF_PIP_INDEX_URL）
ARG PIP_INDEX_URL=https://pypi.org/simple
COPY tools/model-server/${REQUIREMENTS} /opt/model-server/requirements.txt
# torch 只在 PyTorch 官方索引上。哈希固定，索引无法替换内容
RUN pip install --require-hashes -r /opt/model-server/requirements.txt \
    --index-url ${PIP_INDEX_URL} \
    --extra-index-url ${TORCH_INDEX}
COPY tools/model-server/server.py tools/model-server/models.json /opt/model-server/
RUN mkdir /models && chown 65532:65532 /models
USER 65532:65532
ENTRYPOINT ["python", "/opt/model-server/server.py"]
