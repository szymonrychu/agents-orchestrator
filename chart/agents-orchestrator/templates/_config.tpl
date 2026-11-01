{{/*
Chosen Postgres sources. Returns a comma separated list of the sources that are set.
*/}}
{{- define "agents-orchestrator.postgresSources" -}}
{{- $s := list -}}
{{- if .Values.cnpg.enabled }}{{ $s = append $s "cnpg.enabled" }}{{ end -}}
{{- if .Values.postgres.existingSecret }}{{ $s = append $s "postgres.existingSecret" }}{{ end -}}
{{- if .Values.postgres.url }}{{ $s = append $s "postgres.url" }}{{ end -}}
{{- if and .Values.postgres.host .Values.postgres.user .Values.postgres.database }}{{ $s = append $s "postgres.host/user/database" }}{{ end -}}
{{- join "," $s -}}
{{- end }}

{{/*
Validated Postgres source name: one of cnpg, existingSecret, url, parts.
*/}}
{{- define "agents-orchestrator.postgresSource" -}}
{{- $sources := splitList "," (include "agents-orchestrator.postgresSources" .) | compact -}}
{{- if eq (len $sources) 0 -}}
{{- fail "agents-orchestrator: a Postgres connection is required: set postgres.url, postgres.host/user/database/password, postgres.existingSecret, or cnpg.enabled=true" -}}
{{- else if gt (len $sources) 1 -}}
{{- fail (printf "agents-orchestrator: exactly one Postgres source may be set, got %s" (join ", " $sources)) -}}
{{- end -}}
{{- index (dict "cnpg.enabled" "cnpg" "postgres.existingSecret" "existingSecret" "postgres.url" "url" "postgres.host/user/database" "parts") (first $sources) -}}
{{- end }}

{{/*
The connection URI stored in the chart database Secret (sources url and parts).
*/}}
{{- define "agents-orchestrator.databaseUri" -}}
{{- $p := .Values.postgres -}}
{{- if $p.url -}}
{{- $p.url -}}
{{- else -}}
{{- $uri := printf "postgresql://%s:%s@%s:%v/%s" (urlquery $p.user | replace "+" "%20") (urlquery $p.password | replace "+" "%20") $p.host $p.port $p.database -}}
{{- if $p.sslmode }}{{ $uri = printf "%s?sslmode=%s" $uri $p.sslmode }}{{ end -}}
{{- $uri -}}
{{- end -}}
{{- end }}

{{/*
ORCH_DATABASE_URL env entry from the resolved source.
*/}}
{{- define "agents-orchestrator.databaseUrlEnv" -}}
{{- $source := include "agents-orchestrator.postgresSource" . -}}
- name: ORCH_DATABASE_URL
  valueFrom:
    secretKeyRef:
{{- if eq $source "cnpg" }}
      name: {{ include "agents-orchestrator.fullname" . }}-db-app
      key: uri
{{- else if eq $source "existingSecret" }}
      name: {{ .Values.postgres.existingSecret }}
      key: {{ .Values.postgres.existingSecretKey | default "uri" }}
{{- else }}
      name: {{ include "agents-orchestrator.fullname" . }}-database
      key: uri
{{- end }}
{{- end }}

{{/* Name of the Secret holding the master key. */}}
{{- define "agents-orchestrator.masterKeySecretName" -}}
{{- if .Values.masterKey.existingSecret -}}
{{- .Values.masterKey.existingSecret -}}
{{- else if .Values.masterKey.value -}}
{{- include "agents-orchestrator.fullname" . }}-master-key
{{- else -}}
{{- fail "agents-orchestrator: masterKey is required: set masterKey.value or masterKey.existingSecret" -}}
{{- end -}}
{{- end }}

{{/* Name of the Secret holding the Claude OAuth token. */}}
{{- define "agents-orchestrator.claudeSecretName" -}}
{{- if .Values.claude.existingSecret -}}
{{- .Values.claude.existingSecret -}}
{{- else if .Values.claude.token -}}
{{- include "agents-orchestrator.fullname" . }}-claude
{{- else -}}
{{- fail "agents-orchestrator: claude is required: set claude.token or claude.existingSecret" -}}
{{- end -}}
{{- end }}

{{/*
Container env shared by every component: envFrom the settings ConfigMap, the database
URL and the master key. Include inside a container with nindent.
*/}}
{{- define "agents-orchestrator.env" -}}
envFrom:
  - configMapRef:
      name: {{ include "agents-orchestrator.fullname" . }}-settings
env:
{{ include "agents-orchestrator.databaseUrlEnv" . | indent 2 }}
  - name: ORCH_MASTER_KEY
    valueFrom:
      secretKeyRef:
        name: {{ include "agents-orchestrator.masterKeySecretName" . }}
        key: {{ .Values.masterKey.existingSecretKey | default "master-key" }}
{{- if or .Values.serviceApi.existingSecret .Values.serviceApi.tokenHashes }}
  - name: ORCH_SERVICE_TOKENS
    valueFrom:
      secretKeyRef:
        name: {{ include "agents-orchestrator.serviceTokensSecretName" . }}
        key: {{ .Values.serviceApi.existingSecretKey | default "service-tokens" }}
{{- end }}
{{- end }}

{{/* Name of the Secret holding ORCH_SERVICE_TOKENS: existingSecret wins over the chart Secret. */}}
{{- define "agents-orchestrator.serviceTokensSecretName" -}}
{{- default (printf "%s-services" (include "agents-orchestrator.fullname" .)) .Values.serviceApi.existingSecret -}}
{{- end }}

{{/* CSV name:sha256hex of serviceApi.tokenHashes sorted by name; a malformed hash fails the render. */}}
{{- define "agents-orchestrator.serviceTokensCsv" -}}
{{- $pairs := list -}}
{{- range $name := keys .Values.serviceApi.tokenHashes | sortAlpha -}}
{{- $hash := index $.Values.serviceApi.tokenHashes $name | toString -}}
{{- if not (regexMatch "^[0-9a-f]{64}$" $hash) -}}
{{- fail (printf "agents-orchestrator: serviceApi.tokenHashes.%s must be 64 lowercase hex characters (sha256)" $name) -}}
{{- end -}}
{{- $pairs = append $pairs (printf "%s:%s" $name $hash) -}}
{{- end -}}
{{- join "," $pairs -}}
{{- end }}

{{/*
"true" when the chat relay is configured, empty when it is off. chat.url and a token source
(chat.token or chat.existingSecret) go together; one without the other fails the render.
*/}}
{{- define "agents-orchestrator.chatEnabled" -}}
{{- $token := or .Values.chat.token .Values.chat.existingSecret -}}
{{- if and .Values.chat.url $token -}}
true
{{- else if or .Values.chat.url $token -}}
{{- fail "agents-orchestrator: set chat.url together with chat.token or chat.existingSecret, or none of them" -}}
{{- end -}}
{{- end }}

{{/* Name of the Secret holding ORCH_CHAT_TOKEN: existingSecret wins over the chart Secret. */}}
{{- define "agents-orchestrator.chatSecretName" -}}
{{- default (printf "%s-chat" (include "agents-orchestrator.fullname" .)) .Values.chat.existingSecret -}}
{{- end }}

{{/*
ORCH_CHAT_URL and ORCH_CHAT_TOKEN env entries, always together (Settings refuses one without the
other). Only the api main container, which serves the chat relay, includes this; never the shared
ConfigMap, the migrate init containers or the other components. Empty when the chat relay is off.
*/}}
{{- define "agents-orchestrator.chatEnv" -}}
{{- if include "agents-orchestrator.chatEnabled" . -}}
- name: ORCH_CHAT_URL
  value: {{ .Values.chat.url | quote }}
- name: ORCH_CHAT_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "agents-orchestrator.chatSecretName" . }}
      key: {{ .Values.chat.existingSecretKey | default "chat-token" }}
{{- end -}}
{{- end }}

{{/*
Admin relay upstreams (admin.webhook, admin.schedule, admin.connector). Each needs url together
with a token source (token or existingSecret); one without the other fails the render. Emits
"<name>" lines for the configured ones.
*/}}
{{- define "agents-orchestrator.adminUpstreams" -}}
{{- range $name := list "webhook" "schedule" "connector" -}}
{{- $cfg := index ($.Values.admin | default dict) $name | default dict -}}
{{- $token := or $cfg.token $cfg.existingSecret -}}
{{- if and $cfg.url $token }}
{{ $name }}
{{- else if or $cfg.url $token }}
{{- fail (printf "agents-orchestrator: set admin.%s.url together with admin.%s.token or admin.%s.existingSecret, or none of them" $name $name $name) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Name of the Secret holding ORCH_<NAME>_TOKEN of admin upstream .name: existingSecret wins. */}}
{{- define "agents-orchestrator.adminSecretName" -}}
{{- $cfg := index (.root.Values.admin | default dict) .name | default dict -}}
{{- default (printf "%s-admin-%s" (include "agents-orchestrator.fullname" .root) .name) $cfg.existingSecret -}}
{{- end }}

{{/*
ORCH_WEBHOOK/SCHEDULE/CONNECTOR _URL and _TOKEN env entries, each pair together. Only the api main
container includes this; never the ConfigMap, the init containers or the other components.
*/}}
{{- define "agents-orchestrator.adminEnv" -}}
{{- range $name := splitList "\n" (include "agents-orchestrator.adminUpstreams" . | trim) -}}
{{- if $name }}
{{- $cfg := index ($.Values.admin | default dict) $name }}
- name: ORCH_{{ upper $name }}_URL
  value: {{ $cfg.url | quote }}
- name: ORCH_{{ upper $name }}_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "agents-orchestrator.adminSecretName" (dict "root" $ "name" $name) }}
      key: {{ $cfg.existingSecretKey | default "token" }}
{{- end }}
{{- end -}}
{{- end }}

{{/*
ORCH_AGENT_API_URL: the cluster-internal agents Service URL, unless env.ORCH_AGENT_API_URL overrides it.
*/}}
{{- define "agents-orchestrator.agentApiUrl" -}}
{{- default (printf "http://%s-agents.%s.svc:%v" (include "agents-orchestrator.fullname" .) .Release.Namespace .Values.agentApi.port) (index .Values.env "ORCH_AGENT_API_URL") -}}
{{- end }}

{{/*
"true" when agent usage forwarding is configured, empty when it is off. agentUsage.url and
agentUsage.secretName go together; one without the other fails the render. The Secret is
only named here: the chart never holds the ingest token.
*/}}
{{- define "agents-orchestrator.agentUsageEnabled" -}}
{{- $u := .Values.agentUsage | default dict -}}
{{- if and $u.url $u.secretName -}}
true
{{- else if or $u.url $u.secretName -}}
{{- fail "agents-orchestrator: set agentUsage.url together with agentUsage.secretName, or none of them" -}}
{{- end -}}
{{- end }}
