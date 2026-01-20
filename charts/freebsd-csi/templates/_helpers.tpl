{{/*
Expand the name of the chart.
*/}}
{{- define "freebsd-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "freebsd-csi.fullname" -}}
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
{{- define "freebsd-csi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "freebsd-csi.labels" -}}
helm.sh/chart: {{ include "freebsd-csi.chart" . }}
{{ include "freebsd-csi.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "freebsd-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "freebsd-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Controller selector labels
*/}}
{{- define "freebsd-csi.controllerSelectorLabels" -}}
{{ include "freebsd-csi.selectorLabels" . }}
app.kubernetes.io/component: controller
{{- end }}

{{/*
Node selector labels
*/}}
{{- define "freebsd-csi.nodeSelectorLabels" -}}
{{ include "freebsd-csi.selectorLabels" . }}
app.kubernetes.io/component: node
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "freebsd-csi.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "freebsd-csi.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the image reference
*/}}
{{- define "freebsd-csi.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
TLS secret name
*/}}
{{- define "freebsd-csi.tlsSecretName" -}}
{{- if .Values.tls.existingSecret }}
{{- .Values.tls.existingSecret }}
{{- else }}
{{- printf "%s-tls" (include "freebsd-csi.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Namespace to use - prefers .Values.namespace, falls back to .Release.Namespace
*/}}
{{- define "freebsd-csi.namespace" -}}
{{- if .Values.namespace }}
{{- .Values.namespace }}
{{- else }}
{{- .Release.Namespace }}
{{- end }}
{{- end }}

{{/*
Validate required values
*/}}
{{- define "freebsd-csi.validateValues" -}}
{{- if not .Values.agent.endpoint }}
{{- fail "agent.endpoint is required. Set it to your FreeBSD storage server address (e.g., --set agent.endpoint=http://192.168.1.100:50051)" }}
{{- end }}
{{- end }}
