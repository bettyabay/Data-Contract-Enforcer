"""
ViolationAttributor — Phase 2
Traces a validation failure back to its origin using the Week 4 lineage graph
and git history. Writes attributed violations to violation_log/violations.jsonl.

Usage:
    python contracts/attributor.py \
        --report validation_reports/week3_violated.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output violation_log/violations.jsonl
"""

import argparse
import json
import subprocess
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


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


# ── Step 1 — Lineage graph loading ───────────────────────────────────────────

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

def git_log_file(file_path: str, since_days: int = 14) -> list[dict]:
    """
    Run git log on file_path for commits within the last since_days days.
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
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            cwd=Path(__file__).parent.parent,
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


def git_log_repo(since_days: int = 14) -> list[dict]:
    """
    Fallback: return recent commits across the whole repo when a specific
    file path is not trackable (e.g. data files not in git).
    """
    cmd = [
        "git", "log",
        f"--since={since_days} days ago",
        "--format=%H|%an|%ae|%ai|%s",
        "--max-count=10",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
            cwd=Path(__file__).parent.parent,
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


def build_blame_chain(upstream_nodes: list[dict], violated_field: str) -> list[dict]:
    """
    For each upstream node, attempt git log. Score and rank candidates.
    Returns up to 5 candidates ordered by confidence_score descending.
    """
    candidates = []

    for entry in upstream_nodes:
        node_id = entry["node"]
        hop = entry["hop"]

        # Strip the "file::" prefix to get a usable file path
        file_path = node_id.replace("file::", "")

        commits = git_log_file(file_path)

        # If no commits found for this specific file, try repo-level fallback
        # only for the nearest hop (hop == 1)
        if not commits and hop == 1:
            commits = git_log_repo()

        for commit in commits[:3]:  # max 3 commits per file
            candidates.append({
                "file_path": file_path,
                "commit_hash": commit["commit_hash"],
                "author": commit["email"] or commit["author"],
                "commit_timestamp": commit["timestamp"],
                "commit_message": commit["message"],
                "lineage_hop": hop,
                "confidence_score": score_candidate(commit, hop),
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

def compute_blast_radius(source_node: str, forward: dict,
                         record_count: int) -> dict:
    """
    BFS forward from source_node to find all affected downstream nodes.
    Returns blast_radius dict matching spec schema.
    """
    affected = bfs_downstream(source_node, forward)

    # Derive affected pipeline names from node IDs
    pipelines = []
    for node in affected:
        clean = node.replace("file::", "")
        name = Path(clean).stem.replace("_", "-")
        pipelines.append(name)

    return {
        "affected_nodes": affected,
        "affected_pipelines": list(set(pipelines)),
        "estimated_records": record_count,
    }


# ── Main attribution function ─────────────────────────────────────────────────

def attribute_violations(
    report: dict,
    lineage_path: str,
    output_path: str,
) -> list[dict]:
    """
    For every FAIL/CRITICAL result in the report, run full attribution:
    lineage BFS → git blame → confidence scoring → blast radius.
    Appends attributed violations to output_path (JSONL).
    Returns list of written violation dicts.
    """
    snapshot = load_latest_snapshot(lineage_path)
    forward, backward = build_adjacency(snapshot)

    contract_id = report.get("contract_id", "unknown")
    data_path = report.get("data_path", "")
    record_count = report.get("record_count", 0)

    # Find the lineage node for the data source
    source_node = find_source_node(snapshot, data_path)

    # Get upstream nodes once (same for all violations in this report)
    upstream_nodes: list[dict] = []
    if source_node:
        upstream_nodes = bfs_upstream(source_node, backward)

    # Compute blast radius once
    blast = compute_blast_radius(
        source_node or data_path, forward, record_count
    )

    written = []
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    failing_results = [
        r for r in report.get("results", [])
        if r["status"] in ("CRITICAL", "FAIL")
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
            if v["status"] in ("CRITICAL", "FAIL")
        ]

    with open(output_path, "a", encoding="utf-8") as out_f:
        for result in failing_results:
            blame_chain = build_blame_chain(
                upstream_nodes, result.get("column_name", "unknown")
            )

            violation = {
                "violation_id": str(uuid.uuid4()),
                "check_id": result.get("check_id", ""),
                "contract_id": contract_id,
                "detected_at": _now(),
                "field": result.get("column_name", ""),
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
            print(
                f"[attributor] Attributed: [{result['status']}] "
                f"{result.get('column_name')} / {result.get('check_type')} "
                f"-> blast_radius={len(blast['affected_nodes'])} nodes, "
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
    parser.add_argument("--output", default="violation_log/violations.jsonl",
                        help="Path to append attributed violations JSONL")
    args = parser.parse_args()

    with open(args.report, encoding="utf-8") as f:
        report = json.load(f)

    violations = attribute_violations(report, args.lineage, args.output)
    print(f"\n[attributor] Wrote {len(violations)} attributed violation(s) to {args.output}")


if __name__ == "__main__":
    main()
