# Helm 冒烟测试的执行容器（make helm-smoke）：helm、kubectl、curl、jq，加上本仓库的 chart 与冒烟脚本。
# HashiCorp 的两个 chart 由 make 先下载到 tools/helm-smoke/charts/（不入库）。
FROM alpine/k8s:1.33.5
LABEL org.bambuflow.role=helm-smoke
COPY deploy/helm/bambuflow /chart
COPY tools/helm-smoke /smoke
ENTRYPOINT ["/smoke/smoke.sh"]
