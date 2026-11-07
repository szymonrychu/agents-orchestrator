"""The orchestrator's unit is a session: no tracked line calls it a cluster (design D2, D12).

Every tracked file of the repo is guarded. Lines naming a Kubernetes, CNPG or kind cluster stay;
they are matched by the literal phrases of ALLOWED_PHRASES, which are removed from a line before
it is checked. Dated-history files (DATED_HISTORY) and schema history (HISTORY) keep their words.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Kubernetes, CNPG and kind clusters (D12); never the orchestrator's unit.
ALLOWED_PHRASES = (
    "cluster.local",
    "cluster_domain",
    "CLUSTER_DOMAIN",
    "ClusterIP",
    "clusterIP",
    "ClusterIssuer",
    "cluster-issuer",
    "ClusterRoleBinding",
    "ClusterRole",
    "cluster-admin",
    # Kubernetes cluster access of a session (T-55): the option, its setting, chart values,
    # objects and prose; it grants access to the Kubernetes cluster, not a unit of the orchestrator.
    "kube_cluster_access",
    "KUBE_CLUSTER_ACCESS",
    "kube-cluster-access",
    "kubeClusterAccess",
    "KubeClusterAccess",
    "Kube Cluster Access",
    "cluster_granted",
    "cluster_binding_name",
    "cluster access",
    "Cluster access",
    "cluster-access",
    "Cluster-wide",
    "cluster-wide",
    "clusterrolebindings",
    "clusterroles",
    "cluster scope",
    "cluster-scope",
    "CLUSTER_SCOPED",
    "CLUSTER_ADMIN_ROLE",
    "cluster_access",
    "cluster_rbac",
    "cluster_binding",
    "cluster_scoped",
    "cluster_scope",
    "cluster_wins",
    "cluster_to_namespace",
    "and_cluster_together",
    "clusteraccess",
    "clusterrolebinding",
    "Cluster-scoped",
    "cluster-scoped",
    "in-cluster",
    "In-cluster",
    "cluster-internal",
    "Cluster-internal",
    "cluster node",
    "cluster-info",
    "db-cluster",
    "kind cluster",
    "KIND_CLUSTER",
    "kind create cluster",
    "kind delete cluster",
    "kind: Cluster",
    "Kubernetes cluster",
    "CloudNativePG Cluster",
    "config.cluster.server",
    # kubeconfig keys
    '"clusters": [{"name": "k", "cluster": {"server"',
    "clusters: [{name: k, cluster: {server",
    '"context": {"cluster": "k"',
    "context: {cluster: k",
    "cnpg.io/cluster",
    "cnpg-cluster",
    'CNPG_KIND = "Cluster"',
)

# Old names that retirement tests pin as gone (old routes, the old agent-API header), and the
# README's breaking-change notes naming exactly what was renamed.
RETIRED_PHRASES = (
    '"/api/v1/clusters"',
    "test_old_cluster_routes_gone",
    '"X-Orch-Cluster"',
    "test_old_cluster_header_refused",
    "`/api/v1/clusters...`",
    "`cluster_id` field",
    "`ORCH_CLUSTER_ID`",
    "`ORCH_CLUSTER_SLUG`",
    "`X-Orch-Cluster-Id`",
    "`X-Orch-Cluster-Slug`",
    "`X-Orch-Cluster`",
)

# Dated history keeps the words of its time (D12): MEMORY.md, ROADMAP.md, CHANGELOG.md, docs/plans/.
DATED_HISTORY = frozenset({"MEMORY.md", "ROADMAP.md", "CHANGELOG.md"})
DATED_HISTORY_DIRS = ("docs/plans/",)

# Schema history: 0001-0004 stay byte-identical, 0005 renames their old names, and the migration
# tests seed the pre-0005 schema.
HISTORY = frozenset(
    {
        "src/agents_orchestrator/db/migrations/0001_init.sql",
        "src/agents_orchestrator/db/migrations/0002_presets.sql",
        "src/agents_orchestrator/db/migrations/0003_hive.sql",
        "src/agents_orchestrator/db/migrations/0004_wrapper_session_id.sql",
        "src/agents_orchestrator/db/migrations/0005_sessions.sql",
        "tests/test_migrations.py",
        "tests/test_nomenclature.py",
    }
)

UNIT = re.compile("cluster", re.IGNORECASE)


def _tracked() -> list[str]:
    out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True)
    return out.stdout.split()


def _guarded() -> list[str]:
    return [
        p
        for p in _tracked()
        if p not in HISTORY and p not in DATED_HISTORY and not p.startswith(DATED_HISTORY_DIRS)
    ]


def _strip(line: str) -> str:
    for phrase in (*ALLOWED_PHRASES, *RETIRED_PHRASES):
        line = line.replace(phrase, "")
    return line


def _offending(path: str) -> list[str]:
    try:
        text = (ROOT / path).read_text(encoding="utf-8")
    except (UnicodeDecodeError, FileNotFoundError):
        return []
    hits = []
    for n, line in enumerate(text.splitlines(), start=1):
        if UNIT.search(_strip(line)):
            hits.append(f"{path}:{n}: {line.strip()}")
    return hits


def test_every_tracked_file_names_the_unit_session() -> None:
    files = _guarded()
    for path in ("README.md", "AGENTS.md", "scripts/e2e.sh", "e2e/agent-test.config.yaml"):
        assert path in files, path
    assert len(files) > 150
    hits = [hit for path in files for hit in _offending(path)]
    assert not hits, "\n".join(hits[:50]) + f"\n({len(hits)} lines)"


def test_no_tracked_path_names_the_unit() -> None:
    paths = [p for p in _guarded() if UNIT.search(_strip(p))]
    assert paths == []


def test_allowlist_names_only_kubernetes_clusters() -> None:
    for phrase in ALLOWED_PHRASES:
        assert UNIT.search(phrase), phrase
    assert not any(p.lower() in ("cluster", "clusters", "cluster_id") for p in ALLOWED_PHRASES)
