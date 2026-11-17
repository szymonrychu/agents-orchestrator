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

## Agent usage forwarding

`agentUsage` makes the agent pods forward their Claude usage to agents-usage. It is off by
default (both values empty) and renders nothing then.

| Value | Default | Setting |
|---|---|---|
| `agentUsage.url` | `""` (off) | `ORCH_AGENT_USAGE_URL` |
| `agentUsage.secretName` | `""` (off) | `ORCH_AGENT_USAGE_SECRET` |

Set both or neither; one without the other fails the render. `secretName` names a Secret in the
release namespace with the shared ingest token under the key `token` (for agents-usage, the
Secret `agents-usage-ingest`); the agent pods read it as env `CCW_USAGE_TOKEN`. The chart only
names the Secret and never holds the token. Both settings go into the settings ConfigMap. It
needs an `agentImage` of claude-code-wrapper 0.8.0 or newer, because an older wrapper refuses
the `usage` config section; a session on another image (`spec.image`) does not forward.

## Usage gate

The orchestrator can refuse (409 `usage_quota`) to create a session or scale one up while the
Claude plan usage reported by agents-usage is at or over the session's quota. It is off by
default and renders no URL or token then.

| Value | Default | Setting |
|---|---|---|
| `usage.url` | `""` (off) | `ORCH_USAGE_URL` |
| `usage.token` | `""` | `ORCH_USAGE_TOKEN`, via the chart Secret `<fullname>-usage`, key `usage-token` |
| `usage.existingSecret` | `""` | `ORCH_USAGE_TOKEN`, via this Secret (wins over `usage.token`) |
| `usage.existingSecretKey` | `usage-token` | key of the token in the Secret |
| `vault.url` | `""` (off) | `ORCH_VAULT_URL` |
| `vault.token` | `""` | `ORCH_VAULT_TOKEN`, via the chart Secret `<fullname>-vault`, key `vault-token` |
| `vault.existingSecret` | `""` | `ORCH_VAULT_TOKEN`, via this Secret (wins over `vault.token`) |
| `vault.existingSecretKey` | `vault-token` | key of the token in the Secret |

`vault.url` is the agents-vault service listener. Set it together with `vault.token` or `vault.existingSecret`, or none of them; one without the other fails the render. Env of the api main container only.
| `usageQuotaDefault` | `100` | `ORCH_USAGE_QUOTA_DEFAULT` |

`usage.url` is the agents-usage service port; the token is the service token the orchestrator
sends as a bearer (the sha256 of it goes into `serviceApi.tokenHashes` of agents-usage). Set the
URL together with `usage.token` or `usage.existingSecret`, or none of them; one without the other
fails the render. The URL and the token are env of the api main container only, because Settings
refuses a URL without its token and no other component or init container uses the gate; the
settings ConfigMap carries only `ORCH_USAGE_QUOTA_DEFAULT`. A metric gates when its percent is at
or above the limit, also on a stale reading (a lower bound); missing or reset data never gates.

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
| `cache.s3.api.existingSecret` | `""` (the backups routes answer 503) | - |
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
options), used for the backups list, versions, manifest, download, copy, delete and cleanup preview routes. It may name the same Secret as `cache.s3.existingSecret`
when the store has one identity. Without it those routes answer 503.
The store policy of the api key (for example an RGW user policy): `s3:ListBucket`, `s3:GetObject` and `s3:DeleteObject` on the bucket,
and `s3:PutObject` under `<prefix>b/`, because the backup copy route is a server-side CopyObject
and the store checks the write on the destination key. Without `s3:PutObject` the copy answers 502
`storage_error`. The reconciler key needs the same four (it also presigns the Jobs' uploads).
The cleanup sweep starts in
dry-run and must stay in dry-run until the object migration (`orch backup migrate`, see
`backupMigration` below) has run and its restores are verified: read `GET /api/v1/backups/cleanup/preview`,
then set `cache.sweep.dryRun: false`.
The `cache.sweep.*` values are only the seed: they apply until a policy is saved in Settings > Backups
(or with `PUT /api/v1/backups/cleanup/settings`), and from then on the database row wins and later
changes to these values have no effect.
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

### Backup migration Job

`backupMigration.enabled: true` renders a Helm `pre-upgrade` hook Job
`<fullname>-backup-migrate` (default off; it needs `cache.s3.bucket`). Its init container runs
`orch migrate` (SQL, spec and status rewrite) and its container `orch backup migrate` (legacy flat
archives copied into the `b/` layout as versions), with the reconciler's S3 key pair and no
Kubernetes API access. `backupMigration.deleteOld: true` makes the container run
`orch backup migrate --delete-old` instead (a later release, after the restores were verified).
Run order of the release that switches to named backups: `orch backup migrate --dry-run` must exit 0,
scale the old `api` and `reconciler` Deployments to 0, upgrade with `backupMigration.enabled: true`
(the hook finishes before the new Deployments roll), then set it back to `false` in the next upgrade.
The hook is upgrade-only (a fresh install has nothing to migrate) and reads the previous release's
`-settings` ConfigMap, so change `cache.s3` values in a separate upgrade before enabling migration.
Deploy only when no session is mid-restore or mid-save, and with an agent image of
claude-code-wrapper 0.20.0 or newer (`claude.subagent_allowed_tools` is rendered for every session).
