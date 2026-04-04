"""
ViolationAttributor — Phase 2
Traces a validation failure back to its origin.  Runs a four-step pipeline:

  Step 1 — Registry blast radius query (primary): load subscriptions.yaml, find
            every subscriber whose breaking_fields include the failing field.
  Step 2 — Lineage traversal (enrichment): BFS the Week 4 lineage graph for
            transitive contamination depth.
  Step 3 — Git blame: run git log on upstream files to build a ranked blame chain.
  Step 4 — Write violation log: append attributed violation JSONL.

Usage:
    python contracts/attributor.py \
        --report validation_reports/week3_violated.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --registry contract_registry/subscriptions.yaml \
        --output violation_log/violations.jsonl
"""

import argparse
import json
import subprocess
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _days_since(ts_str: str) -> float:
    """Return fractional days between ts_str (ISO 8601) and now."""
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return max(0.0, (now - ts).total_seconds() / 86400)
    except Exception:
        return 14.0  # default if unparseable


# ── Step 1 — Registry blast radius (primary) ─────────────────────────────────

def load_registry(registry_path: str) -> list[dict]:
    """Load contract registry subscriptions from YAML. Returns [] if not found."""
    p = Path(registry_path)
    if not p.exists():
        return []
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("subscriptions", [])


def _normalize_field(field: str) -> str:
    """Strip [*] array markers for comparison: 'extracted_facts[*].confidence' → 'extracted_facts.confidence'"""
    return field.replace("[*]", "")


def registry_blast_radius(
    contract_id: str, failing_field: str, subscriptions: list[dict]
) -> list[dict]:
    """
    Query the registry for every subscriber affected by a breaking field change.
    A subscriber is affected when its breaking_fields list contains the failing field
    (full match or prefix match, e.g. 'extracted_facts.confidence' matches
    'extracted_facts' and vice-versa).
    [*] array markers are normalized before comparison.

    Returns a list of subscriber dicts for inclusion in blast_radius.
    """
    affected = []
    norm_failing = _normalize_field(failing_field)
    for sub in subscriptions:
        if sub.get("contract_id") != contract_id:
            continue
        breaking_fields = sub.get("breaking_fields", [])
        # breaking_fields entries may be dicts {"field": ..., "reason": ...} or plain strings
        field_names = [
            _normalize_field(bf["field"] if isinstance(bf, dict) else str(bf))
            for bf in breaking_fields
        ]
        # Match if the failing field is equal to, a prefix of, or a suffix of any breaking field
        match = any(
            norm_failing == f
            or norm_failing.startswith(f + ".")
            or f.startswith(norm_failing + ".")
            for f in field_names
        )
        if match:
            affected.append({
                "subscriber_id": sub.get("subscriber_id"),
                "subscriber_team": sub.get("subscriber_team"),
                "contact": sub.get("contact"),
                "validation_mode": sub.get("validation_mode"),
                "fields_consumed": sub.get("fields_consumed", []),
            })
    return affected


# ── Step 2 — Lineage graph loading ───────────────────────────────────────────

def load_latest_snapshot(lineage_path: str) -> dict:
    """Return the most recent snapshot from a lineage JSONL file."""
    with open(lineage_path, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    if not lines:
        return {"nodes": [], "edges": []}
    return json.loads(lines[-1])


def build_adjacency(snapshot: dict) -> tuple[dict, dict]:
    """
    Build forward (source→targets) and backward (target→sources) adjacency maps.
    Returns (forward, backward) dicts keyed by node_id.
    """
    forward: dict[str, list[str]] = {}
    backward: dict[str, list[str]] = {}
    for edge in snapshot.get("edges", []):
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        forward.setdefault(src, []).append(tgt)
        backward.setdefault(tgt, []).append(src)
    return forward, backward


# ── Step 2 — BFS traversal ────────────────────────────────────────────────────

def bfs_upstream(start_node: str, backward: dict, max_hops: int = 4) -> list[dict]:
    """
    BFS from start_node following backward (target←source) edges.
    Returns list of {node, hop} dicts ordered nearest-first.
    Stops at external service boundaries or max_hops.
    """
    visited = {start_node}
    queue = deque([(start_node, 0)])
    upstream = []

    while queue:
        node, hop = queue.popleft()
        if hop >= max_hops:
            continue
        for parent in backward.get(node, []):
            if parent not in visited:
                visited.add(parent)
                upstream.append({"node": parent, "hop": hop + 1})
                queue.append((parent, hop + 1))

    return upstream


def bfs_downstream(start_node: str, forward: dict, max_hops: int = 4) -> list[str]:
    """
    BFS from start_node following forward (source→target) edges.
    Returns list of affected node IDs.
    """
    visited = {start_node}
    queue = deque([start_node])
    affected = []

    while queue:
        node = queue.popleft()
        for child in forward.get(node, []):
            if child not in visited:
                visited.add(child)
                affected.append(child)
                queue.append(child)

    return affected


def find_source_node(snapshot: dict, contract_source_path: str) -> str | None:
    """
    Find the lineage graph node that corresponds to the contract's source file.
    Matches on path substring.
    """
    key = Path(contract_source_path).name  # e.g. "extractions.jsonl"
    for node in snapshot.get("nodes", []):
        nid = node.get("node_id", "")
        if key in nid or contract_source_path in nid:
            return nid
    return None


# ── Step 3 — Git blame integration ───────────────────────────────────────────

def git_log_file(file_path: str, since_days: int = 14, repo_root: str = None) -> list[dict]:
    """
    Run git log on file_path for commits within the last since_days days.
    repo_root should be the codebase_root from the lineage snapshot so that
    the relative file path resolves correctly in the right repository.
    Returns list of {commit_hash, author, email, timestamp, message} dicts.
    """
    since = f"{since_days} days ago"
    cmd = [
        "git", "log",
        f"--since={since}",
        "--follow",
        "--format=%H|%an|%ae|%ai|%s",
        "--",
        file_path,
    ]
    cwd = repo_root if repo_root else str(Path(__file__).parent.parent)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            cwd=cwd,
        )
        commits = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("|", 4)
            if len(parts) == 5:
                commits.append({
                    "commit_hash": parts[0],
                    "author": parts[1],
                    "email": parts[2],
                    "timestamp": parts[3].strip(),
                    "message": parts[4],
                })
        return commits
    except Exception:
        return []


def git_blame_file(file_path: str, line_start: int = 1, line_end: int = 30,
                   repo_root: str = None) -> set[str]:
    """
    Run git blame -L {line_start},{line_end} --porcelain on file_path.
    repo_root should be the codebase_root from the lineage snapshot.
    Returns the set of commit hashes that appear in those lines.
    Used to corroborate git log results: if a commit hash from git log also
    appears in git blame output, that commit is more likely the causal change.
    """
    cmd = [
        "git", "blame",
        f"-L{line_start},{line_end}",
        "--porcelain",
        file_path,
    ]
    cwd = repo_root if repo_root else str(Path(__file__).parent.parent)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            cwd=cwd,
        )
        hashes: set[str] = set()
        for line in result.stdout.splitlines():
            # Porcelain format: lines starting with a 40-char hex hash
            parts = line.split()
            if parts and len(parts[0]) == 40 and all(c in "0123456789abcdef" for c in parts[0]):
                hashes.add(parts[0])
        return hashes
    except Exception:
        return set()


def git_log_repo(since_days: int = 14, repo_root: str = None) -> list[dict]:
    """
    Fallback: return recent commits across the whole repo when a specific
    file path is not trackable (e.g. data files not in git).
    repo_root should be the codebase_root from the lineage snapshot.
    """
    cmd = [
        "git", "log",
        f"--since={since_days} days ago",
        "--format=%H|%an|%ae|%ai|%s",
        "--max-count=10",
    ]
    cwd = repo_root if repo_root else str(Path(__file__).parent.parent)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            cwd=cwd,
        )
        commits = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("|", 4)
            if len(parts) == 5:
                commits.append({
                    "commit_hash": parts[0],
                    "author": parts[1],
                    "email": parts[2],
                    "timestamp": parts[3].strip(),
                    "message": parts[4],
                })
        return commits
    except Exception:
        return []


# ── Step 4 — Confidence scoring ───────────────────────────────────────────────

def score_candidate(commit: dict, lineage_hop: int) -> float:
    """
    confidence_score = 1.0 − (days_since_commit × 0.1) − (lineage_hop × 0.2)
    Clamped to [0.01, 1.0].
    """
    days = _days_since(commit.get("timestamp", ""))
    recency_penalty = days * 0.1
    distance_penalty = lineage_hop * 0.2
    score = 1.0 - recency_penalty - distance_penalty
    return round(max(0.01, min(1.0, score)), 4)


def build_blame_chain(upstream_nodes: list[dict], violated_field: str,
                      repo_root: str = None) -> list[dict]:
    """
    For each upstream node, attempt git log. Score and rank candidates.
    repo_root is the codebase_root from the lineage snapshot — git commands
    run inside that directory so relative node paths resolve correctly.
    Returns up to 5 candidates ordered by confidence_score descending.
    """
    candidates = []

    for entry in upstream_nodes:
        node_id = entry["node"]
        hop = entry["hop"]

        # Strip the "file::" prefix to get a usable file path
        file_path = node_id.replace("file::", "")

        commits = git_log_file(file_path, repo_root=repo_root)

        # If no commits found for this specific file, try repo-level fallback
        # only for the nearest hop (hop == 1)
        if not commits and hop == 1:
            commits = git_log_repo(repo_root=repo_root)

        # git blame -L (line-level attribution, first 30 lines of the file).
        # Commit hashes that appear in blame output corroborate git log results.
        blame_hashes = git_blame_file(file_path, line_start=1, line_end=30,
                                      repo_root=repo_root)

        for commit in commits[:3]:  # max 3 commits per file
            base_score = score_candidate(commit, hop)
            # Boost by 0.05 (capped at 1.0) when the commit appears in blame
            if commit["commit_hash"] in blame_hashes:
                base_score = min(1.0, base_score + 0.05)
            candidates.append({
                "file_path": file_path,
                "commit_hash": commit["commit_hash"],
                "author": commit["email"] or commit["author"],
                "commit_timestamp": commit["timestamp"],
                "commit_message": commit["message"],
                "lineage_hop": hop,
                "confidence_score": round(base_score, 4),
            })

    if not candidates:
        # No git history found — produce a synthetic placeholder so the
        # violation record is still structurally valid
        candidates.append({
            "file_path": "unknown",
            "commit_hash": "0" * 40,
            "author": "unknown",
            "commit_timestamp": _now(),
            "commit_message": f"No git history found for field '{violated_field}' upstream",
            "lineage_hop": 0,
            "confidence_score": 0.01,
        })

    # Sort by confidence descending, keep top 5, assign rank
    candidates.sort(key=lambda c: c["confidence_score"], reverse=True)
    ranked = []
    for i, c in enumerate(candidates[:5], start=1):
        c["rank"] = i
        ranked.append(c)

    return ranked


# ── Step 5 — Blast radius ─────────────────────────────────────────────────────

def _max_contamination_depth(start_node: str, forward: dict) -> int:
    """Return the maximum BFS depth reachable from start_node in the forward graph."""
    visited = {start_node}
    queue = deque([(start_node, 0)])
    max_depth = 0
    while queue:
        node, depth = queue.popleft()
        max_depth = max(max_depth, depth)
        for child in forward.get(node, []):
            if child not in visited:
                visited.add(child)
                queue.append((child, depth + 1))
    return max_depth


def compute_blast_radius(
    source_node: str,
    forward: dict,
    record_count: int,
    contract_id: str = "",
    failing_field: str = "",
    subscriptions: list = None,
) -> dict:
    """
    Compute blast radius using a two-source model:
      Primary   — registry subscribers (authoritative for who declared a dependency).
      Enrichment — lineage BFS (transitive contamination depth from the data graph).

    Returns blast_radius dict matching the updated spec schema.
    """
    # Primary: registry subscribers
    registry_subs: list[dict] = []
    if subscriptions and contract_id and failing_field:
        registry_subs = registry_blast_radius(contract_id, failing_field, subscriptions)

    # Enrichment: lineage BFS
    affected_lineage = bfs_downstream(source_node, forward) if source_node else []
    contamination_depth = (
        _max_contamination_depth(source_node, forward) if affected_lineage else 0
    )

    # Derive pipeline names from lineage node IDs
    pipelines = [
        Path(n.replace("file::", "")).stem.replace("_", "-")
        for n in affected_lineage
    ]

    return {
        "registry_subscribers": registry_subs,
        "affected_nodes": affected_lineage,
        "affected_pipelines": list(set(pipelines)),
        "estimated_records": record_count,
        "contamination_depth": contamination_depth,
    }


# ── Main attribution function ─────────────────────────────────────────────────

def attribute_violations(
    report: dict,
    lineage_path: str,
    output_path: str,
    registry_path: str = None,
) -> list[dict]:
    """
    Four-step attribution pipeline per the new spec:
      1. Registry blast radius query (primary subscriber list).
      2. Lineage BFS enrichment (contamination depth).
      3. Git blame for blame chain construction.
      4. Write violation log JSONL.

    Appends attributed violations to output_path (JSONL).
    Returns list of written violation dicts.
    """
    # Step 1: load registry subscriptions (primary blast radius source)
    subscriptions = load_registry(registry_path) if registry_path else []

    snapshot = load_latest_snapshot(lineage_path)
    forward, backward = build_adjacency(snapshot)

    # codebase_root is the repo that Week 4 analyzed — git commands must run
    # there so relative node paths (e.g. "file::src/foo.py") resolve correctly.
    repo_root: str | None = snapshot.get("codebase_root")

    contract_id = report.get("contract_id", "unknown")
    data_path = report.get("data_path", "")
    record_count = report.get("record_count", 0)

    # Find the lineage node for the data source
    source_node = find_source_node(snapshot, data_path)

    # Get upstream nodes once (same for all violations in this report)
    upstream_nodes: list[dict] = []
    if source_node:
        upstream_nodes = bfs_upstream(source_node, backward)

    written = []
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    failing_results = [
        r for r in report.get("results", [])
        if r["status"] == "FAIL"
    ]

    # Fall back to legacy violations list if results not present
    if not failing_results:
        failing_results = [
            {
                "check_id": f"{contract_id}.{v['field']}.{v['check']}",
                "column_name": v["field"],
                "check_type": v["check"],
                "status": v["status"],
                "message": v["reason"],
                "records_failing": record_count,
            }
            for v in report.get("violations", [])
            if v["status"] == "FAIL"
        ]

    with open(output_path, "a", encoding="utf-8") as out_f:
        for result in failing_results:
            failing_field = result.get("column_name", "")
            blame_chain = build_blame_chain(
                upstream_nodes, failing_field, repo_root=repo_root
            )

            # Per-violation blast radius: registry query on the specific failing field
            blast = compute_blast_radius(
                source_node or data_path, forward, record_count,
                contract_id=contract_id,
                failing_field=failing_field,
                subscriptions=subscriptions,
            )

            violation = {
                "violation_id": str(uuid.uuid4()),
                "check_id": result.get("check_id", ""),
                "contract_id": contract_id,
                "detected_at": _now(),
                "field": failing_field,
                "check_type": result.get("check_type", ""),
                "status": result["status"],
                "message": result.get("message", ""),
                "records_failing": result.get("records_failing", 0),
                "blame_chain": blame_chain,
                "blast_radius": blast,
                "confidence_score": blame_chain[0]["confidence_score"] if blame_chain else 0.0,
            }

            out_f.write(json.dumps(violation) + "\n")
            written.append(violation)
            registry_count = len(blast.get("registry_subscribers", []))
            lineage_count = len(blast.get("affected_nodes", []))
            print(
                f"[attributor] Attributed: [{result['status']}] "
                f"{failing_field} / {result.get('check_type')} "
                f"-> registry_subscribers={registry_count}, "
                f"lineage_nodes={lineage_count} (depth={blast.get('contamination_depth', 0)}), "
                f"top_candidate_score={violation['confidence_score']}"
            )

    return written


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Attribute violations in a validation report to source commits."
    )
    parser.add_argument("--report", required=True, help="Path to validation report JSON")
    parser.add_argument("--lineage", required=True, help="Path to lineage snapshots JSONL")
    parser.add_argument(
        "--registry",
        default="contract_registry/subscriptions.yaml",
        help="Path to contract_registry/subscriptions.yaml (primary blast radius source)",
    )
    parser.add_argument("--output", default="violation_log/violations.jsonl",
                        help="Path to append attributed violations JSONL")
    args = parser.parse_args()

    with open(args.report, encoding="utf-8") as f:
        report = json.load(f)

    violations = attribute_violations(
        report, args.lineage, args.output, registry_path=args.registry
    )
    print(f"\n[attributor] Wrote {len(violations)} attributed violation(s) to {args.output}")


if __name__ == "__main__":
    main()
