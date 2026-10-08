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
