{{/*
Expand the name of the chart.
*/}}
{{- define "velocity.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "velocity.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "velocity.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "velocity.labels" -}}
helm.sh/chart: {{ include "velocity.chart" . }}
{{ include "velocity.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "velocity.selectorLabels" -}}
app.kubernetes.io/name: {{ include "velocity.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "velocity.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "velocity.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
PostgreSQL secret name
*/}}
{{- define "velocity.postgresSecretName" -}}
{{- if .Values.postgresql.auth.existingSecret }}
{{- .Values.postgresql.auth.existingSecret }}
{{- else }}
{{- printf "%s-postgres" (include "velocity.fullname" .) }}
{{- end }}
{{- end }}

{{/*
PostgreSQL fully-qualified name
*/}}
{{- define "velocity.postgresFullname" -}}
{{- printf "%s-postgresql" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Prometheus fully-qualified name
*/}}
{{- define "velocity.prometheusFullname" -}}
{{- printf "%s-prometheus" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Grafana fully-qualified name
*/}}
{{- define "velocity.grafanaFullname" -}}
{{- printf "%s-grafana" (include "velocity.fullname" .) }}
{{- end }}

{{/*
PostgreSQL connection string
*/}}
{{- define "velocity.postgresConnectionString" -}}
{{- if .Values.postgresql.enabled -}}
postgresql://{{ .Values.postgresql.auth.username }}:$(POSTGRES_PASSWORD)@{{ include "velocity.postgresFullname" . }}:{{ .Values.postgresql.port }}/{{ .Values.postgresql.auth.database }}
{{- else -}}
postgresql://{{ .Values.postgresql.auth.username }}:$(POSTGRES_PASSWORD)@{{ .Values.postgresql.host | default (include "velocity.postgresFullname" .) }}:{{ .Values.postgresql.port }}/{{ .Values.postgresql.auth.database }}
{{- end }}
{{- end }}

{{/*
Velero fully-qualified name
*/}}
{{- define "velocity.veleroFullname" -}}
{{- printf "%s-velero" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Jaeger fully-qualified name
*/}}
{{- define "velocity.jaegerFullname" -}}
{{- printf "%s-jaeger" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Tempo fully-qualified name
*/}}
{{- define "velocity.tempoFullname" -}}
{{- printf "%s-tempo" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Loki fully-qualified name
*/}}
{{- define "velocity.lokiFullname" -}}
{{- printf "%s-loki" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Redis fully-qualified name
*/}}
{{- define "velocity.redisFullname" -}}
{{- printf "%s-redis" (include "velocity.fullname" .) }}
{{- end }}

{{/*
Alertmanager fully-qualified name
*/}}
{{- define "velocity.alertmanagerFullname" -}}
{{- printf "%s-alertmanager" (include "velocity.fullname" .) }}
{{- end }}
