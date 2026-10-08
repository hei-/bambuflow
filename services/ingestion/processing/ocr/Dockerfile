# processing 的 OCR 镜像（技术设计文档 12.2：OCR 单独镜像、跑在独立 CPU 节点池）。接口见 server.py。
# 依赖按 requirements.txt 的 sha256 安装；PP-OCRv5 检测（mobile）与识别（server）模型在构建时按 sha256 下载，运行时不联网。
FROM python:3.12-slim-bookworm
LABEL org.bambuflow.role=service
# 默认官方的 PyPI 与 Debian 源；网络访问不了时由构建参数换成镜像（.env.local 的 BF_PIP_INDEX_URL、BF_DEBIAN_MIRROR）。
# PyPI 包按哈希固定、Debian 包由 apt 验签，镜像无法替换内容
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG DEBIAN_MIRROR=http://deb.debian.org
# PaddleX 导入时默认探测各模型源的连通性；模型已在镜像里，关掉探测，运行时不访问外网。
# 以非 root 用户运行时没有家目录，PaddlePaddle 的缓存写到 /tmp
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=True HOME=/tmp
# libgomp1：PaddlePaddle 的 OpenMP；libgl1、libglib2.0-0：OpenCV（PaddleX 依赖的是带 GUI 模块的版本）
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends libgomp1 libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*
COPY services/ingestion/processing/ocr/requirements.txt /opt/ocr/requirements.txt
# requirements.txt 是 uv 解析好的完整依赖清单（含 overrides.txt 对 OpenCV 的覆盖），按清单原样安装，
# 不让 pip 重新按各包声明的依赖求解（否则 PaddleX 对 OpenCV 的精确钉住会与覆盖冲突）
RUN pip install --no-deps --require-hashes -r /opt/ocr/requirements.txt --index-url ${PIP_INDEX_URL}
ADD --checksum=sha256:50446e5d01ac2a73d5319c89513281f6578414c888c602f9af13f93feefffc58 \
    https://paddle-model-ecology.bj.bcebos.com/paddlex/official_inference_model/paddle3.0.0/PP-OCRv5_mobile_det_infer.tar /tmp/det.tar
ADD --checksum=sha256:d99be2ffd348943ab52876179168be4fb5b14f5f0812f2ae4c76d89ec2ea750a \
    https://paddle-model-ecology.bj.bcebos.com/paddlex/official_inference_model/paddle3.0.0/PP-OCRv5_server_rec_infer.tar /tmp/rec.tar
RUN mkdir -p /opt/ocr/models && tar -xf /tmp/det.tar -C /opt/ocr/models && tar -xf /tmp/rec.tar -C /opt/ocr/models \
    && rm /tmp/det.tar /tmp/rec.tar
COPY services/ingestion/processing/ocr/server.py /opt/ocr/server.py
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["python", "/opt/ocr/server.py"]
CMD ["serve"]
