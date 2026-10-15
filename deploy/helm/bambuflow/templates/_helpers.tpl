{{/* 组件的资源名：<release>-<组件> */}}
{{- define "bambuflow.name" -}}
{{ .root.Release.Name }}-{{ .component }}
{{- end -}}

{{- define "bambuflow.labels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/part-of: bambuflow
app.kubernetes.io/version: {{ .root.Values.image.tag | quote }}
{{- end -}}

{{- define "bambuflow.selector" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end -}}

{{/* Prometheus 按注解抓取各 Pod 的 /metrics（技术设计文档 14.2；端口与 telemetry 库的 BF_METRICS_LISTEN 默认值一致） */}}
{{- define "bambuflow.metricsAnnotations" -}}
prometheus.io/scrape: "true"
prometheus.io/port: "9464"
prometheus.io/path: /metrics
{{- end -}}

{{/* 监控命名空间 → 指标端口（参数是监控命名空间名） */}}
{{- define "bambuflow.metricsIngress" -}}
- ports:
    - port: 9464
  from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: {{ . }}
{{- end -}}
