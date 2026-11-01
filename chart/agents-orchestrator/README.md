# agents-orchestrator chart

Installs the agents-orchestrator api, dispatcher, ingester and reconciler into the one namespace
it manages. The full guide (install, Postgres, secrets, RBAC, ingress, security) is the root
`README.md`; this file covers the values that need deployment-specific input.

## Allowed origins

The api refuses every WebSocket upgrade (the terminal) whose `Origin` header is `null` or not
listed in `ORCH_ALLOWED_ORIGINS`, because browsers apply no CORS to WebSocket upgrades. The list
is empty by default, which refuses every browser upgrade, so a deployment that serves the UI must
list the UI origin through `env`:

```yaml
env:
  # production: the ingress origin the browser loads the UI from
  ORCH_ALLOWED_ORIGINS: https://orchestrator.example.com
  # local UI development against the Vite dev server (http://localhost:5173):
  # ORCH_ALLOWED_ORIGINS: http://localhost:5173
```

Entries are comma-separated `scheme://host[:port]` origins, lower-case, without a path or a
trailing slash. `env` reaches every component's ConfigMap unchanged.

## Ingress timeouts for WebSockets

ingress-nginx closes idle proxied connections after 60 s. Raise both timeouts through
`ingress.annotations`; the UI pings every 20 s, so 3600 keeps an idle terminal open:

```yaml
ingress:
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
```

## Workspace cache

The reconciler can restore a session workspace from, and save it to, any S3-compatible object
store through short-lived cache Jobs. Setting `cache.s3.bucket` enables it; empty (the default)
renders no `ORCH_CACHE_*` setting and the cache stays off.

| Value | Default | Setting |
|---|---|---|
| `cache.s3.endpoint` | `""` (the AWS endpoint of the region) | `ORCH_CACHE_S3_ENDPOINT` |
| `cache.s3.region` | `us-east-1` | `ORCH_CACHE_S3_REGION` |
| `cache.s3.bucket` | `""` (cache off) | `ORCH_CACHE_S3_BUCKET` |
| `cache.s3.prefix` | `workspace-cache/` | `ORCH_CACHE_S3_PREFIX` |
| `cache.s3.pathStyle` | `false` | `ORCH_CACHE_S3_PATH_STYLE` |
| `cache.s3.verifyTls` | `true` | `ORCH_CACHE_S3_VERIFY_TLS` |
| `cache.s3.existingSecret` | `""` (required with a bucket) | - |
| `cache.s3.accessKeyIdKey` | `AWS_ACCESS_KEY_ID` | `ORCH_CACHE_S3_ACCESS_KEY_ID` |
| `cache.s3.secretAccessKeyKey` | `AWS_SECRET_ACCESS_KEY` | `ORCH_CACHE_S3_SECRET_ACCESS_KEY` |
| `cache.image` | `""` (the chart image, `image.repository:image.tag`) | `ORCH_CACHE_IMAGE` |
| `cache.maxBytes` | `5368709120` (5 GiB, the maximum) | `ORCH_CACHE_MAX_BYTES` |
| `cache.timeoutSeconds` | `1800` | `ORCH_CACHE_TIMEOUT_SECONDS` |
| `cache.s3.api.existingSecret` | `""` (the archives routes answer 503) | - |
| `cache.s3.api.accessKeyIdKey` | `AWS_ACCESS_KEY_ID` | `ORCH_CACHE_S3_API_ACCESS_KEY_ID` |
| `cache.s3.api.secretAccessKeyKey` | `AWS_SECRET_ACCESS_KEY` | `ORCH_CACHE_S3_API_SECRET_ACCESS_KEY` |
| `cache.sweep.enabled` | `true` | `ORCH_CACHE_SWEEP_ENABLED` |
| `cache.sweep.dryRun` | `true` | `ORCH_CACHE_SWEEP_DRY_RUN` |
| `cache.sweep.retentionDays` | `30` | `ORCH_CACHE_RETENTION_DAYS` |
| `cache.sweep.keepNewest` | `3` | `ORCH_CACHE_KEEP_NEWEST` |
| `cache.sweep.intervalSeconds` | `21600` | `ORCH_CACHE_SWEEP_INTERVAL_SECONDS` |
| `cache.sweep.graceSeconds` | `""` (`timeoutSeconds` + 600) | `ORCH_CACHE_SWEEP_GRACE_SECONDS` |

Everything but the access keys goes into the settings ConfigMap. The access key pair is read
from `cache.s3.existingSecret` (keys `cache.s3.accessKeyIdKey` and `cache.s3.secretAccessKeyKey`)
and reaches the reconciler container only, as `secretKeyRef` env: the api, dispatcher and
ingester, the migrate init containers and the agent pods never see it, and each cache Job gets
one presigned URL instead. A bucket without `cache.s3.existingSecret` fails the render.
The api container alone reads its own pair from `cache.s3.api.existingSecret` (same key
options), used for the archives list, download, cleanup preview and delete routes; the code
only lists, gets and deletes with it. It may name the same Secret as `cache.s3.existingSecret`
when the store has one identity. Without it those routes answer 503. The cleanup sweep starts in
dry-run: read `GET /api/v1/cache/cleanup/preview`, then set `cache.sweep.dryRun: false`.
The reconciler Role allows create, get and delete on `batch` Jobs in the release
namespace (Kubernetes RBAC cannot restrict verbs by label).

```yaml
cache:
  s3:
    endpoint: https://s3.example.com
    bucket: workspace-cache
    pathStyle: true
    existingSecret: workspace-cache-s3
```
