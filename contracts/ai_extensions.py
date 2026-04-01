"""
AI Contract Extensions — Phase 2
Three AI-specific contract checks not covered by standard tabular validation:
  1. Embedding drift    — cosine distance from centroid baseline (threshold 0.15)
  2. Prompt validation  — input schema enforcement + quarantine
  3. Output violation rate — LLM output conformance rate (warn at 2%)

Usage:
    python contracts/ai_extensions.py \
        --traces outputs/traces/runs.jsonl \
        --contract generated_contracts/langsmith_traces.yaml \
        --baselines schema_snapshots/ai_baselines.json \
        --output validation_reports/ai_extensions.json
"""

import argparse
import json
import math
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

from contracts.generator import load_jsonl


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _cosine_distance(a: list[float], b: list[float]) -> float:
    """Cosine distance = 1 - cosine_similarity. Range [0, 2]."""
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(y * y for y in b))
    if mag_a == 0 or mag_b == 0:
        return 1.0
    return 1.0 - (dot / (mag_a * mag_b))


def _mean_vector(vectors: list[list[float]]) -> list[float]:
    """Element-wise mean of a list of equal-length vectors."""
    if not vectors:
        return []
    n = len(vectors[0])
    centroid = [0.0] * n
    for v in vectors:
        for i, x in enumerate(v):
            centroid[i] += x
    return [x / len(vectors) for x in centroid]


def _simple_embedding(record: dict) -> list[float]:
    """
    Lightweight pseudo-embedding from a trace record.
    Uses [latency_ms_norm, total_tokens_norm, confidence, error_flag]
    so checks work without a real embedding model.
    Replace with actual model embeddings in production.
    """
    latency = record.get("latency_ms", 0) / 10000.0      # normalise to ~[0,1]
    tokens = record.get("total_tokens", 0) / 5000.0       # normalise
    conf = record.get("outputs", {}).get("confidence", 0.5)
    if isinstance(record.get("outputs"), dict):
        conf = record["outputs"].get("confidence", 0.5)
    else:
        conf = 0.5
    error_flag = 0.0 if record.get("error") is None else 1.0
    return [latency, tokens, conf, error_flag]


# ── Check 1 — Embedding drift ─────────────────────────────────────────────────

def check_embedding_drift(
    records: list[dict],
    baselines: dict,
    results: list,
    threshold: float = 0.15,
) -> dict:
    """
    Compute pseudo-embeddings for each trace record. Compare batch centroid
    against stored baseline centroid. Emit WARN if cosine distance > threshold.
    Returns updated baselines dict with 'embedding_centroid' entry.
    """
    vectors = [_simple_embedding(r) for r in records]
    if not vectors:
        return baselines

    current_centroid = _mean_vector(vectors)

    new_baselines = dict(baselines)

    if "embedding_centroid" not in baselines:
        # First run — store baseline, no violation
        new_baselines["embedding_centroid"] = {
            "centroid": current_centroid,
            "established_at": _now(),
            "record_count": len(records),
        }
        results.append({
            "check_id": "langsmith.embeddings.drift",
            "check_type": "embedding_drift",
            "status": "PASS",
            "actual_value": f"centroid established from {len(records)} records",
            "expected": f"cosine_distance <= {threshold}",
            "severity": "LOW",
            "records_failing": 0,
            "message": "Embedding baseline established. No prior baseline to compare against.",
        })
        return new_baselines

    baseline_centroid = baselines["embedding_centroid"]["centroid"]
    dist = _cosine_distance(current_centroid, baseline_centroid)

    status = "PASS"
    severity = "LOW"
    message = f"Embedding centroid cosine distance from baseline: {dist:.4f} (threshold={threshold})"

    if dist > threshold:
        status = "WARN"
        severity = "MEDIUM"
        message = (
            f"Embedding drift detected: cosine distance={dist:.4f} exceeds threshold={threshold}. "
            f"LLM behaviour may have shifted — review recent model or prompt changes."
        )

    results.append({
        "check_id": "langsmith.embeddings.drift",
        "check_type": "embedding_drift",
        "status": status,
        "actual_value": f"cosine_distance={dist:.4f}",
        "expected": f"cosine_distance <= {threshold}",
        "severity": severity,
        "records_failing": len(records) if status != "PASS" else 0,
        "message": message,
    })

    return new_baselines


# ── Check 2 — Prompt input schema validation ──────────────────────────────────

# Required fields in the inputs dict of every trace record
_REQUIRED_INPUT_FIELDS = {"prompt", "context"}
_PROMPT_MIN_LENGTH = 10
_QUARANTINE_DIR = "outputs/quarantine"


def check_prompt_schema(
    records: list[dict],
    results: list,
    quarantine_dir: str = _QUARANTINE_DIR,
) -> int:
    """
    Validate that every trace record's inputs dict has required fields and
    that the prompt is non-trivially short.
    Quarantines invalid records to outputs/quarantine/trace_quarantine.jsonl.
    Returns count of quarantined records.
    """
    quarantined = []
    bad_count = 0

    for record in records:
        inputs = record.get("inputs", {})
        issues = []

        for field in _REQUIRED_INPUT_FIELDS:
            if field not in inputs:
                issues.append(f"missing required input field '{field}'")

        prompt = inputs.get("prompt", "")
        if isinstance(prompt, str) and len(prompt) < _PROMPT_MIN_LENGTH:
            issues.append(
                f"prompt too short ({len(prompt)} chars < {_PROMPT_MIN_LENGTH} minimum)"
            )

        if issues:
            bad_count += 1
            quarantined.append({
                "run_id": record.get("run_id", "unknown"),
                "quarantined_at": _now(),
                "issues": issues,
                "record": record,
            })

    if quarantined:
        Path(quarantine_dir).mkdir(parents=True, exist_ok=True)
        qpath = Path(quarantine_dir) / "trace_quarantine.jsonl"
        with open(qpath, "a", encoding="utf-8") as f:
            for q in quarantined:
                f.write(json.dumps(q) + "\n")

    status = "FAIL" if bad_count > 0 else "PASS"
    severity = "HIGH" if bad_count > 0 else "LOW"
    results.append({
        "check_id": "langsmith.inputs.schema",
        "check_type": "prompt_input_schema",
        "status": status,
        "actual_value": f"{bad_count} records with invalid inputs",
        "expected": f"all records have inputs.prompt (>={_PROMPT_MIN_LENGTH} chars) and inputs.context",
        "severity": severity,
        "records_failing": bad_count,
        "message": (
            f"{bad_count} record(s) failed prompt input schema validation and were quarantined to {quarantine_dir}."
            if bad_count > 0
            else "All prompt input schemas are valid."
        ),
    })

    return bad_count


# ── Check 3 — LLM output violation rate ──────────────────────────────────────

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

WARN_RATE = 0.02   # 2%
FAIL_RATE = 0.05   # 5%


def _validate_output(record: dict) -> list[str]:
    """
    Check a single trace record's outputs dict against expected schema.
    Returns list of issue strings (empty = valid).
    """
    issues = []
    outputs = record.get("outputs", {})

    if not isinstance(outputs, dict):
        issues.append("outputs is not a dict")
        return issues

    result = outputs.get("result")
    if result is None:
        issues.append("outputs.result is missing")

    confidence = outputs.get("confidence")
    if confidence is not None:
        try:
            c = float(confidence)
            if c < 0.0 or c > 1.0:
                issues.append(
                    f"outputs.confidence={c} outside 0.0-1.0 range — "
                    "possible scale change (BREAKING)"
                )
        except (TypeError, ValueError):
            issues.append(f"outputs.confidence is not numeric: {confidence!r}")

    return issues


def check_output_violation_rate(
    records: list[dict],
    baselines: dict,
    results: list,
    warn_rate: float = WARN_RATE,
    fail_rate: float = FAIL_RATE,
) -> dict:
    """
    Compute the fraction of trace records with invalid outputs.
    WARN if rate > warn_rate (2%), FAIL if rate > fail_rate (5%).
    Compares against baseline rate if available.
    """
    if not records:
        return baselines

    invalid_count = 0
    sample_issues = []

    for record in records:
        issues = _validate_output(record)
        if issues:
            invalid_count += 1
            if len(sample_issues) < 5:
                sample_issues.append({
                    "run_id": record.get("run_id", "?"),
                    "issues": issues,
                })

    current_rate = invalid_count / len(records)
    new_baselines = dict(baselines)

    baseline_rate = baselines.get("output_violation_rate", {}).get("rate")

    if baseline_rate is None:
        # First run — store baseline
        new_baselines["output_violation_rate"] = {
            "rate": current_rate,
            "established_at": _now(),
            "record_count": len(records),
        }
        results.append({
            "check_id": "langsmith.outputs.violation_rate",
            "check_type": "output_violation_rate",
            "status": "PASS",
            "actual_value": f"rate={current_rate:.4f} ({invalid_count}/{len(records)})",
            "expected": f"rate <= {warn_rate}",
            "severity": "LOW",
            "records_failing": invalid_count,
            "message": f"Output violation rate baseline established: {current_rate:.2%}.",
        })
        return new_baselines

    # Compare against baseline
    if current_rate > fail_rate:
        status, severity = "FAIL", "HIGH"
        msg = (
            f"Output violation rate {current_rate:.2%} exceeds FAIL threshold {fail_rate:.2%}. "
            f"Baseline was {baseline_rate:.2%}. "
            f"Sample issues: {sample_issues[:2]}"
        )
    elif current_rate > warn_rate:
        status, severity = "WARN", "MEDIUM"
        msg = (
            f"Output violation rate {current_rate:.2%} exceeds WARN threshold {warn_rate:.2%}. "
            f"Baseline was {baseline_rate:.2%}."
        )
    else:
        status, severity = "PASS", "LOW"
        msg = f"Output violation rate {current_rate:.2%} is within acceptable bounds."

    results.append({
        "check_id": "langsmith.outputs.violation_rate",
        "check_type": "output_violation_rate",
        "status": status,
        "actual_value": f"rate={current_rate:.4f} ({invalid_count}/{len(records)})",
        "expected": f"rate <= {warn_rate} (WARN), rate <= {fail_rate} (FAIL)",
        "severity": severity,
        "records_failing": invalid_count,
        "message": msg,
    })

    return new_baselines


# ── Baseline persistence ──────────────────────────────────────────────────────

def load_ai_baselines(path: str) -> dict:
    p = Path(path)
    if p.exists():
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return {}


def write_ai_baselines(path: str, baselines: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(baselines, f, indent=2)


# ── Main runner ───────────────────────────────────────────────────────────────

def run_ai_checks(
    traces_path: str,
    contract_path: str,
    baselines_path: str = "schema_snapshots/ai_baselines.json",
    output_path: str = "validation_reports/ai_extensions.json",
    drift_threshold: float = 0.15,
) -> dict:
    """
    Run all three AI extension checks on a traces JSONL file.
    Returns the full check report.
    """
    records = load_jsonl(traces_path)
    baselines = load_ai_baselines(baselines_path)
    results: list[dict] = []

    # Check 1: Embedding drift
    baselines = check_embedding_drift(records, baselines, results, drift_threshold)

    # Check 2: Prompt input schema
    check_prompt_schema(records, results)

    # Check 3: Output violation rate
    baselines = check_output_violation_rate(records, baselines, results)

    # Persist updated baselines
    write_ai_baselines(baselines_path, baselines)

    # Determine overall status
    statuses = {r["status"] for r in results}
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "WARN"
    else:
        overall = "PASS"

    report = {
        "report_id": str(uuid.uuid4()),
        "check_category": "ai_extensions",
        "traces_path": traces_path,
        "record_count": len(records),
        "run_timestamp": _now(),
        "overall_status": overall,
        "total_checks": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "warned": sum(1 for r in results if r["status"] == "WARN"),
        "failed": sum(1 for r in results if r["status"] == "FAIL"),
        "results": results,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run AI-specific contract checks on LangSmith traces."
    )
    parser.add_argument("--traces", required=True, help="Path to traces JSONL file")
    parser.add_argument("--contract", required=True, help="Path to langsmith contract YAML")
    parser.add_argument("--baselines", default="schema_snapshots/ai_baselines.json")
    parser.add_argument("--output", default="validation_reports/ai_extensions.json")
    parser.add_argument("--drift-threshold", type=float, default=0.15)
    args = parser.parse_args()

    report = run_ai_checks(
        traces_path=args.traces,
        contract_path=args.contract,
        baselines_path=args.baselines,
        output_path=args.output,
        drift_threshold=args.drift_threshold,
    )

    print(f"\n[ai_extensions] Overall status : {report['overall_status']}")
    print(f"[ai_extensions] Checks run     : {report['total_checks']} "
          f"(passed={report['passed']}, warned={report['warned']}, failed={report['failed']})")
    print(f"[ai_extensions] Report written : {args.output}")
    for r in report["results"]:
        print(f"  [{r['status']}] {r['check_type']}: {r['message'][:120]}")


if __name__ == "__main__":
    main()
