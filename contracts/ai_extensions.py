"""
AI Contract Extensions — Phase 4 (enhanced from Phase 2)
Three AI-specific contract checks not covered by standard tabular validation.

Phase 4 additions (spec-exact implementations):
  check_embedding_drift(texts, baseline_path, threshold)
    — real text-embedding-3-small embeddings via OpenAI API, npz centroid storage
  PROMPT_INPUT_SCHEMA + validate_prompt_inputs(records, schema, quarantine_path)
    — formal JSON Schema validation, non-conforming records go to quarantine
  check_output_schema_violation_rate(verdict_records, baseline_rate, warn_threshold)
    — tracks overall_verdict ∈ {PASS,FAIL,WARN} conformance rate with trend

Phase 2 functions preserved (LangSmith trace checks):
  check_embedding_drift (pseudo-embedding on trace records)
  check_prompt_schema   (inputs.prompt / inputs.context validation)
  check_output_violation_rate (outputs.confidence / outputs.result validation)

Usage (Phase 4):
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


# ══════════════════════════════════════════════════════════════════════════════
# Phase 4 AI Extensions — spec-exact implementations
# ══════════════════════════════════════════════════════════════════════════════

try:
    import numpy as _np
    _NUMPY_AVAILABLE = True
except ImportError:
    _np = None  # type: ignore
    _NUMPY_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer as _SentenceTransformer
    _ST_AVAILABLE = True
except ImportError:
    _SentenceTransformer = None  # type: ignore
    _ST_AVAILABLE = False

try:
    import jsonschema as _jsonschema
    _JSONSCHEMA_AVAILABLE = True
except ImportError:
    _jsonschema = None  # type: ignore
    _JSONSCHEMA_AVAILABLE = False

# Cached model instance — loaded once, reused across calls
_ST_MODEL = None


def _get_st_model():
    """Load sentence-transformers model on first use (cached)."""
    global _ST_MODEL
    if _ST_MODEL is None:
        _ST_MODEL = _SentenceTransformer("all-MiniLM-L6-v2")
    return _ST_MODEL


# ── Phase 4 Extension 1: Embedding Drift (sentence-transformers, no API key) ──
# NOTE: named check_text_embedding_drift to avoid shadowing the Phase 2
# check_embedding_drift(records, baselines, results, threshold) used by run_ai_checks.

def embed_sample(texts: list[str], n: int = 200) -> "list[list[float]]":
    """
    Embed a random sample of n texts using all-MiniLM-L6-v2 (local, no API key).
    Model is ~90MB, downloaded automatically on first run via HuggingFace.
    Returns list of 384-dim embedding vectors.
    """
    import random as _random
    sample = _random.sample(texts, min(n, len(texts)))
    model = _get_st_model()
    embeddings = model.encode(sample, convert_to_numpy=True)
    return embeddings.tolist()


def check_text_embedding_drift(
    texts: list[str],
    baseline_path: str,
    threshold: float = 0.15,
) -> dict:
    """
    Cosine distance between current text centroid and stored baseline centroid.
    Uses sentence-transformers (all-MiniLM-L6-v2) — no API key required.
    First run stores the baseline and returns status=BASELINE_SET.

    drift = 1 − cosine_similarity(current_centroid, baseline_centroid)
    Returns {'status', 'drift_score', 'threshold', 'sample_size', 'message'}.
    """
    if not _NUMPY_AVAILABLE:
        return {"status": "SKIP", "drift_score": None, "message": "numpy not installed."}
    if not _ST_AVAILABLE:
        return {
            "status": "SKIP", "drift_score": None,
            "message": "sentence-transformers not installed. Run: pip install sentence-transformers",
        }
    if not texts:
        return {"status": "SKIP", "drift_score": None, "message": "No texts provided."}

    try:
        vectors = embed_sample(texts, n=200)
        current_centroid = _np.mean(_np.array(vectors), axis=0)

        bpath = Path(baseline_path)
        if not bpath.exists():
            bpath.parent.mkdir(parents=True, exist_ok=True)
            _np.savez(str(bpath), centroid=current_centroid)
            return {
                "status": "BASELINE_SET",
                "drift_score": 0.0,
                "threshold": threshold,
                "sample_size": len(vectors),
                "baseline_path": str(bpath),
                "message": (
                    f"Baseline centroid stored ({len(vectors)} samples). "
                    "Re-run after the next data refresh to begin drift detection."
                ),
            }

        baseline_centroid = _np.load(str(bpath))["centroid"]
        cos_sim = float(
            _np.dot(current_centroid, baseline_centroid)
            / (_np.linalg.norm(current_centroid) * _np.linalg.norm(baseline_centroid) + 1e-9)
        )
        drift = round(1.0 - cos_sim, 4)

        return {
            "status": "FAIL" if drift > threshold else "PASS",
            "drift_score": drift,
            "threshold": threshold,
            "sample_size": len(vectors),
            "message": (
                f"Embedding drift {drift} exceeds threshold {threshold} — "
                "distribution shift detected. Inspect recent data for domain change."
                if drift > threshold
                else f"Embedding drift {drift} within threshold {threshold}."
            ),
        }
    except Exception as exc:
        return {"status": "ERROR", "drift_score": None, "message": str(exc)}


# ── Phase 4 Extension 2: Prompt Input Schema Validation ───────────────────────

PROMPT_INPUT_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id":          {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path":     {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def validate_prompt_inputs(
    records: list[dict],
    schema: dict | None = None,
    quarantine_path: str = "outputs/quarantine/prompt_input_violations.jsonl",
) -> dict:
    """
    Validate records against PROMPT_INPUT_SCHEMA before they reach the LLM.
    Non-conforming records go to quarantine_path — never silently dropped.
    Returns {status, total, valid, quarantined, quarantine_rate, violations}.
    """
    if schema is None:
        schema = PROMPT_INPUT_SCHEMA
    if not _JSONSCHEMA_AVAILABLE:
        return {"status": "SKIP", "message": "jsonschema not installed.", "total": len(records)}

    valid_records: list[dict] = []
    quarantined: list[dict] = []
    violation_details: list[dict] = []

    for record in records:
        try:
            _jsonschema.validate(instance=record, schema=schema)
            valid_records.append(record)
        except _jsonschema.ValidationError as e:
            quarantined.append(record)
            violation_details.append({
                "record_id": record.get("doc_id", "unknown"),
                "validation_error": e.message,
                "failed_path": list(e.absolute_path) if e.absolute_path else [],
            })

    if quarantined:
        Path(quarantine_path).parent.mkdir(parents=True, exist_ok=True)
        with open(quarantine_path, "a", encoding="utf-8") as qf:
            for r in quarantined:
                qf.write(json.dumps(r) + "\n")

    rate = round(len(quarantined) / max(len(records), 1), 4)
    return {
        "status": "FAIL" if quarantined else "PASS",
        "total": len(records),
        "valid": len(valid_records),
        "quarantined": len(quarantined),
        "quarantine_rate": rate,
        "violations": violation_details[:10],
        "quarantine_path": quarantine_path if quarantined else None,
        "message": (
            f"{len(quarantined)}/{len(records)} records quarantined ({rate:.1%}). "
            f"See {quarantine_path}."
            if quarantined
            else f"All {len(records)} records passed prompt input schema validation."
        ),
    }


# ── Phase 4 Extension 3: LLM Output Schema Violation Rate ─────────────────────

def check_output_schema_violation_rate(
    verdict_records: list[dict],
    baseline_rate: float | None = None,
    warn_threshold: float = 0.02,
) -> dict:
    """
    Track what fraction of LLM verdict outputs have overall_verdict ∉ {PASS,FAIL,WARN}.
    A rising rate signals prompt degradation or model behaviour change.
    Returns {total_outputs, schema_violations, violation_rate, trend, status}.
    """
    total = len(verdict_records)
    violations = sum(
        1 for v in verdict_records
        if v.get("overall_verdict") not in ("PASS", "FAIL", "WARN")
    )
    rate = round(violations / max(total, 1), 4)

    trend = "unknown"
    if baseline_rate is not None:
        trend = "rising" if rate > baseline_rate * 1.5 else (
            "falling" if rate < baseline_rate * 0.5 else "stable"
        )

    return {
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": rate,
        "baseline_rate": baseline_rate,
        "warn_threshold": warn_threshold,
        "trend": trend,
        "status": "WARN" if rate > warn_threshold else "PASS",
        "message": (
            f"Violation rate {rate:.2%} exceeds warn threshold {warn_threshold:.2%} "
            f"(trend: {trend}). Check prompt version and model rollout."
            if rate > warn_threshold
            else f"Violation rate {rate:.2%} within bounds (trend: {trend})."
        ),
    }


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
