# Compose 环境的 Prometheus 与 Grafana（只用于开发与演示，技术设计文档 14.2）：远端 Docker 不能挂载本地目录，
# 抓取配置、告警规则、数据源与仪表盘随 build 打进镜像。
FROM prom/prometheus:v3.5.0 AS prometheus
LABEL org.bambuflow.role=observability
COPY deploy/observability/prometheus/prometheus.yml /etc/prometheus/prometheus.yml
COPY deploy/observability/rules/ /etc/prometheus/rules/

FROM grafana/grafana:12.1.0 AS grafana
LABEL org.bambuflow.role=observability
COPY deploy/observability/grafana/provisioning/ /etc/grafana/provisioning/
COPY deploy/observability/grafana/dashboards/ /var/lib/grafana/dashboards/
