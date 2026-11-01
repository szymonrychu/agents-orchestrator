#!/usr/bin/env bash
# kind end-to-end (mise run test:e2e): a kind cluster, both images built and loaded, an RWX
# StorageClass, a plain Postgres, the chart installed into namespace agents with
# e2e/values-kind.yaml, helm test, then tests/e2e against the port-forwarded api Service.
# The kind cluster is deleted on exit (E2E_KEEP=1 keeps it for debugging).
#
# Environment:
#   E2E_KEEP=1             keep the kind cluster (and its kubeconfig) after the run
#   E2E_PORT               local port of the api port-forward (default 18080)
#   E2E_AGENT_BASE_IMAGE   claude-code-wrapper image the test agent image builds on; default:
#                          the BASE_IMAGE of e2e/agent-test.Dockerfile when it pulls for this
#                          machine's platform, else built from the wrapper repo at WRAPPER_SHA
#   E2E_HOLD_FILE          bring the environment up, then instead of running pytest write its
#                          variables (KUBECONFIG, ORCH_E2E_*) to this file and hold it until
#                          the file is removed; tests/e2e/conftest.py uses this when a test is
#                          run on its own (pytest tests/e2e/test_e2e_kind.py::<name>)
#   E2E_HOLD_PID           with E2E_HOLD_FILE: stop holding once this process has exited
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KIND_CLUSTER=agents-orchestrator-e2e
NS=agents
RELEASE=agents-orchestrator
ORCH_IMAGE=agents-orchestrator:e2e
AGENT_IMAGE=agents-orchestrator-agent-test:e2e
# a second tag of the same image: the roll-mid-turn e2e upgrades the release to it
AGENT_IMAGE_NEXT=agents-orchestrator-agent-test:e2e-next
# the dind sidecar image; keep it equal to docker.image in e2e/values-kind.yaml
DIND_IMAGE=docker:27.3.1-dind
PORT="${E2E_PORT:-18080}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/orch-e2e.XXXXXX")"
export KUBECONFIG="$WORK/kubeconfig"
PF_PID=""

log() { printf '\n==> %s\n' "$*"; }

diagnostics() {
  log "diagnostics"
  kubectl get nodes -o wide || true
  kubectl get storageclass,pv || true
  kubectl -n "$NS" get all,pvc,configmap,secret -o wide || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -60 || true
  for c in api dispatcher ingester reconciler; do
    echo "--- $RELEASE-$c"
    kubectl -n "$NS" logs "deployment/$RELEASE-$c" --all-containers --tail=120 || true
  done
  kubectl -n "$NS" describe pods -l app.kubernetes.io/managed-by=agents-orchestrator || true
  kubectl -n "$NS" logs -l app.kubernetes.io/managed-by=agents-orchestrator --tail=120 || true
  [ -f "$WORK/port-forward.log" ] && cat "$WORK/port-forward.log" || true
}

cleanup() {
  local rc=$?
  set +e
  [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null
  if [ "$rc" -ne 0 ] && [ -f "$KUBECONFIG" ]; then diagnostics; fi
  if [ "${E2E_KEEP:-}" = 1 ]; then
    log "keeping kind cluster $KIND_CLUSTER (KUBECONFIG=$KUBECONFIG)"
  else
    log "deleting kind cluster $KIND_CLUSTER"
    kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1
    rm -rf "$WORK"
  fi
  exit "$rc"
}
trap cleanup EXIT
# a signal exits through cleanup too, so the kind cluster never outlives the run
trap 'exit 143' TERM INT HUP

dockerfile_arg() {
  sed -n "s/^ARG $1=//p" e2e/agent-test.Dockerfile | head -1
}

ARCH="$(docker version --format '{{.Server.Arch}}')"
PLATFORM="linux/$ARCH"

# The test agent image (fake-wrapper-contract) is the claude-code-wrapper image plus the
# wrapper's fake claude. The Harbor image is built for linux/amd64 only and needs a login, so
# when it does not pull for this platform the same wrapper commit is built from source.
agent_base_image() {
  if [ -n "${E2E_AGENT_BASE_IMAGE:-}" ]; then
    echo "$E2E_AGENT_BASE_IMAGE"
    return
  fi
  local base sha
  base="$(dockerfile_arg BASE_IMAGE)"
  sha="$(dockerfile_arg WRAPPER_SHA)"
  # docker pull --platform can succeed with another platform's image (a fallback, or an
  # image already in the store), so the pulled image's own platform decides.
  if docker pull --quiet --platform "$PLATFORM" "$base" >&2 \
    && [ "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$base")" = "$PLATFORM" ]; then
    echo "$base"
    return
  fi
  echo "cannot pull $base for $PLATFORM; building the wrapper at $sha from source" >&2
  local local_base="agents-orchestrator-e2e-wrapper-base:${sha:0:7}"
  docker build --quiet -t "$local_base" \
    "https://github.com/szymonrychu/claude-code-wrapper.git#$sha" >&2
  echo "$local_base"
}

load_image() {
  # docker save --platform keeps multi-platform image stores (containerd snapshotter) from
  # exporting an index whose other platforms are missing, which kind cannot import.
  local image="$1" tar
  tar="$WORK/$(echo "$image" | tr '/:' '__').tar"
  # Older daemons have no --platform; their single-platform images save as they are.
  docker save --platform "$PLATFORM" -o "$tar" "$image" 2>/dev/null \
    || docker save -o "$tar" "$image"
  kind load image-archive --name "$KIND_CLUSTER" "$tar"
  rm -f "$tar"
}

# Probe a StorageClass: an RWX claim binds and two pods mounting it see each other's file.
probe_rwx() {
  local sc="$1" probe="rwx-probe"
  kubectl delete pvc,pod -n default -l e2e-probe=rwx --ignore-not-found --wait=true >/dev/null
  kubectl apply -n default -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $probe, labels: {e2e-probe: rwx}}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: $sc
  resources: {requests: {storage: 1Mi}}
---
apiVersion: v1
kind: Pod
metadata: {name: $probe-writer, labels: {e2e-probe: rwx}}
spec:
  restartPolicy: Never
  securityContext: {runAsUser: 1000, fsGroup: 1000}
  containers:
    - name: w
      image: busybox
      command: [sh, -c, "echo shared > /data/probe"]
      volumeMounts: [{name: data, mountPath: /data}]
  volumes: [{name: data, persistentVolumeClaim: {claimName: $probe}}]
---
apiVersion: v1
kind: Pod
metadata: {name: $probe-reader, labels: {e2e-probe: rwx}}
spec:
  restartPolicy: OnFailure
  securityContext: {runAsUser: 1000, fsGroup: 1000}
  containers:
    - name: r
      image: busybox
      command: [sh, -c, "grep -qx shared /data/probe"]
      volumeMounts: [{name: data, mountPath: /data}]
  volumes: [{name: data, persistentVolumeClaim: {claimName: $probe}}]
EOF
  local ok=0
  if kubectl wait -n default "pvc/$probe" --for=jsonpath='{.status.phase}'=Bound --timeout=90s \
    && kubectl wait -n default "pod/$probe-writer" \
      --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s \
    && kubectl wait -n default "pod/$probe-reader" \
      --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s; then
    ok=1
  fi
  kubectl delete pod,pvc -n default -l e2e-probe=rwx --ignore-not-found --wait=false >/dev/null
  [ "$ok" = 1 ]
}

# Option 1: kind's local-path-provisioner in shared-filesystem mode (it then accepts
# ReadWriteMany). One node, so a node directory is a shared filesystem.
setup_local_path_shared() {
  local node shared=/var/local-path-provisioner-shared
  for node in $(kind get nodes --name "$KIND_CLUSTER"); do
    docker exec "$node" mkdir -p "$shared"
    docker exec "$node" chmod 0777 "$shared"
  done
  kubectl -n local-path-storage patch configmap local-path-config --type merge \
    -p "{\"data\":{\"config.json\":\"{\\\"sharedFileSystemPath\\\":\\\"$shared\\\"}\"}}"
  kubectl -n local-path-storage rollout restart deployment/local-path-provisioner
  kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=120s
  kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: e2e-rwx}
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: Immediate
EOF
}

# Option 2: a no-provisioner StorageClass with pre-created world-writable hostPath PVs.
setup_hostpath() {
  local node i
  for node in $(kind get nodes --name "$KIND_CLUSTER"); do
    for i in 0 1 2 3; do
      docker exec "$node" mkdir -p "/var/e2e-rwx/pv$i"
      docker exec "$node" chmod 0777 "/var/e2e-rwx/pv$i"
    done
  done
  kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: e2e-rwx-hostpath}
provisioner: kubernetes.io/no-provisioner
reclaimPolicy: Retain
volumeBindingMode: Immediate
EOF
  for i in 0 1 2 3; do
    kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata: {name: e2e-rwx-pv$i}
spec:
  capacity: {storage: 5Gi}
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: e2e-rwx-hostpath
  hostPath: {path: /var/e2e-rwx/pv$i, type: Directory}
EOF
  done
}

choose_rwx_storage_class() {
  setup_local_path_shared >&2 || true
  if probe_rwx e2e-rwx >&2; then
    echo e2e-rwx
    return
  fi
  echo "local-path shared-filesystem mode did not provide RWX; using hostPath PVs" >&2
  setup_hostpath >&2
  probe_rwx e2e-rwx-hostpath >&2
  # the probe claim consumed one Retain PV; free it for the agent sessions
  kubectl get pv -o name | while read -r pv; do
    if [ "$(kubectl get "$pv" -o jsonpath='{.status.phase}')" = Released ]; then
      kubectl patch "$pv" --type json -p '[{"op":"remove","path":"/spec/claimRef"}]' >/dev/null
    fi
  done
  echo e2e-rwx-hostpath
}

log "creating kind cluster $KIND_CLUSTER"
kind delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
kind create cluster --config e2e/kind-config.yaml --kubeconfig "$KUBECONFIG" --wait 180s

log "building $ORCH_IMAGE"
docker build -t "$ORCH_IMAGE" .

log "building $AGENT_IMAGE"
BASE="$(agent_base_image)"
echo "agent base image: $BASE"
mkdir -p "$WORK/agent-context/e2e"
cp e2e/agent-test.scenario.json "$WORK/agent-context/e2e/"
docker build -f e2e/agent-test.Dockerfile --build-arg "BASE_IMAGE=$BASE" \
  -t "$AGENT_IMAGE" "$WORK/agent-context"
docker tag "$AGENT_IMAGE" "$AGENT_IMAGE_NEXT"

log "loading images into kind"
load_image "$ORCH_IMAGE"
load_image "$AGENT_IMAGE"
load_image "$AGENT_IMAGE_NEXT"
docker pull --quiet --platform "$PLATFORM" "$DIND_IMAGE" >&2
load_image "$DIND_IMAGE"

log "probing a ReadWriteMany StorageClass"
STORAGE_CLASS="$(choose_rwx_storage_class)"
echo "ORCH_PVC_STORAGE_CLASS=$STORAGE_CLASS"

log "namespace $NS and the e2e Postgres"
kubectl create namespace "$NS"
# credentials are generated per run; nothing secret-shaped is committed
gen_secret() { openssl rand -hex 16; }
PG_PASSWORD="$(gen_secret)"
MASTER_KEY="$(gen_secret)"
CLAUDE_TOKEN="$(gen_secret)"
kubectl -n "$NS" create secret generic postgres-credentials --from-literal="password=$PG_PASSWORD"
kubectl -n "$NS" apply -f e2e/manifests/postgres.yaml
kubectl -n "$NS" rollout status deployment/postgres --timeout=300s

log "helm install $RELEASE"
helm install "$RELEASE" chart/agents-orchestrator -n "$NS" -f e2e/values-kind.yaml \
  --set-string "env.ORCH_PVC_STORAGE_CLASS=$STORAGE_CLASS" \
  --set-string "postgres.url=postgresql://orch:$PG_PASSWORD@postgres:5432/orch" \
  --set-string "masterKey.value=$MASTER_KEY" \
  --set-string "claude.token=$CLAUDE_TOKEN" --wait --timeout 10m

log "helm test $RELEASE"
helm test "$RELEASE" -n "$NS" --timeout 5m

# A helm upgrade replaces the api pod, which ends a port-forward bound to it (the roll-mid-turn
# e2e upgrades the release), so the forward is re-established until the script exits. The
# TERM trap takes the running kubectl down with the loop.
port_forward() {
  local child=""
  trap 'if [ -n "$child" ]; then kill "$child" 2>/dev/null; fi; exit 0' TERM
  while true; do
    kubectl -n "$NS" port-forward "svc/$RELEASE" "$PORT:80" >>"$WORK/port-forward.log" 2>&1 &
    child=$!
    wait "$child" || true
    sleep 1
  done
}

log "port-forwarding svc/$RELEASE to 127.0.0.1:$PORT"
port_forward &
PF_PID=$!
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$PORT/api/v1/meta" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "http://127.0.0.1:$PORT/api/v1/meta" >/dev/null

E2E_ENV=(
  "ORCH_E2E_API_URL=http://127.0.0.1:$PORT"
  "ORCH_E2E_NAMESPACE=$NS"
  "ORCH_E2E_RELEASE=$RELEASE"
  "ORCH_E2E_KUBE_CONTEXT=kind-$KIND_CLUSTER"
  "ORCH_E2E_AGENT_IMAGE_NEXT=$AGENT_IMAGE_NEXT"
)

if [ -n "${E2E_HOLD_FILE:-}" ]; then
  log "environment up; holding it until $E2E_HOLD_FILE is removed"
  { echo "KUBECONFIG=$KUBECONFIG"; printf '%s\n' "${E2E_ENV[@]}"; } >"$E2E_HOLD_FILE.tmp"
  mv "$E2E_HOLD_FILE.tmp" "$E2E_HOLD_FILE"
  while [ -e "$E2E_HOLD_FILE" ]; do
    if [ -n "${E2E_HOLD_PID:-}" ] && ! kill -0 "$E2E_HOLD_PID" 2>/dev/null; then
      echo "holder $E2E_HOLD_PID exited; tearing down" >&2
      break
    fi
    if ! kill -0 "$PF_PID" 2>/dev/null; then
      echo "the api port-forward exited" >&2
      exit 1
    fi
    sleep 1
  done
  exit 0
fi

log "pytest -m e2e tests/e2e"
# -rA: a "PASSED <node id>" line per test whatever the verbosity (pyproject's addopts -q
# cancels a -v), so the output names every test and none has to be re-run alone to prove it
env "${E2E_ENV[@]}" uv run pytest -m e2e tests/e2e -p no:cacheprovider -rA
