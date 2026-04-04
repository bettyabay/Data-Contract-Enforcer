"""
ValidationRunner — Phase 2
Validates a JSONL data file against a Bitol contract and produces a structured report.

Enforcement modes (--mode flag):
  AUDIT   — Run all checks, log results, never block. Always exits 0. (default)
  WARN    — Block (exit 1) only when CRITICAL-severity violations are found.
  ENFORCE — Block (exit 1) when CRITICAL or HIGH severity violations are found.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_$(date +%Y%m%d_%H%M).json \
        --mode AUDIT
"""

import argparse
import hashlib
import json
import re
import uuid as _uuid_mod
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from contracts.generator import load_jsonl, flatten_for_profile


# ── Column-name helpers (flattened pandas name → original JSON path) ──────────

# Reverse of ARRAY_FIELDS in generator.py (array fields use short prefixes).
# Dict fields (token_count, metadata) flatten as "{key}_{subkey}" in generator.py,
# so their prefix is the full parent key + underscore.
# Order matters: longer prefixes must precede shorter ones to avoid false matches.
_FLAT_TO_JSON_PATH: dict[str, str] = {
    "token_count_": "token_count.",        # token_count_input → token_count.input
    "metadata_":    "metadata.",           # metadata_user_id  → metadata.user_id
    "fact_":        "extracted_facts[*].", # fact_confidence   → extracted_facts[*].confidence
    "entity_":      "entities[*].",        # entity_name       → entities[*].name
    "ref_":         "code_refs[*].",       # ref_confidence    → code_refs[*].confidence
    "node_":        "nodes[*].",           # node_node_id      → nodes[*].node_id
    "edge_":        "edges[*].",           # edge_source       → edges[*].source
}

# Top-level primary-key column names to try when no prefix matches.
_TOP_LEVEL_ID_COLS = (
    "doc_id", "event_id", "intent_id", "verdict_id",
    "snapshot_id", "run_id", "id",
)


def _to_json_path(col: str) -> str:
    """
    Convert a flattened column name to its original JSON path notation.
      'fact_confidence'   → 'extracted_facts[*].confidence'
      'fact_fact_id'      → 'extracted_facts[*].fact_id'
      'token_count_input' → 'token_count.input'
      'doc_id'            → 'doc_id'  (top-level, unchanged)
    """
    for prefix, json_path in _FLAT_TO_JSON_PATH.items():
        if col.startswith(prefix):
            return json_path + col[len(prefix):]
    return col


def _sample_failing_ids(
    df: pd.DataFrame, col: str, bad_series: pd.Series
) -> list[str]:
    """
    For a range-failing Series (index is a valid subset of df.index), return up
    to 5 record identifiers so evaluation scripts can pinpoint the failing rows.

    Strategy:
      1. If col has a known array prefix (e.g. 'fact_'), find a sibling *_id
         column in the same group (e.g. 'fact_fact_id') and return those values.
      2. Fall back to a well-known top-level primary key (doc_id, event_id, …).
      3. Fall back to string representations of the failing values themselves.
    """
    # 1. Sibling ID column in the same flattened array group
    for prefix in ("fact_", "entity_", "ref_", "node_", "edge_"):
        if col.startswith(prefix):
            sibling_ids = [
                c for c in df.columns
                if c.startswith(prefix) and c.endswith("_id")
            ]
            if sibling_ids:
                vals = df.loc[bad_series.index, sibling_ids[0]].dropna()
                if not vals.empty:
                    return vals.head(5).astype(str).tolist()
            break

    # 2. Top-level primary key
    for pk in _TOP_LEVEL_ID_COLS:
        if pk in df.columns:
            vals = df.loc[bad_series.index, pk].dropna()
            if not vals.empty:
                return vals.head(5).astype(str).tolist()

    # 3. Actual failing values
    return [str(v) for v in bad_series.head(5).values]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except FileNotFoundError:
        h.update(path.encode())
    return h.hexdigest()


def _check_id(contract_id: str, col: str, check_type: str) -> str:
    """
    Produce a dotted check identifier: contract.column_json_path.check_type
    Uses the original JSON path for the column part so check IDs match the spec
    format, e.g. 'week3_extractions.extracted_facts.confidence.range'.
    The [*] array marker is stripped to keep the ID clean for logging.
    """
    json_col = _to_json_path(col).replace("[*]", "")
    return f"{contract_id}.{json_col}.{check_type}"


def _severity(status: str, check_type: str = "") -> str:
    """
    Map status + check_type to a severity level.
    Structural/type violations always CRITICAL regardless of status value.
    Spec: CRITICAL (structural or type violation), HIGH (statistical drift >3 stddev),
          MEDIUM (statistical drift 2–3 stddev), LOW (informational), WARNING (near-threshold).
    """
    _structural = {"required_field_present", "type_match", "range"}
    if check_type in _structural:
        return "CRITICAL"
    mapping = {
        "FAIL": "HIGH",
        "WARN": "MEDIUM",
        "ERROR": "LOW",
    }
    return mapping.get(status, "LOW")


def _make_result(check_id: str, col: str, check_type: str, status: str,
                 actual: str, expected: str, message: str,
                 records_failing: int = 0, sample_failing: list = None,
                 severity: str = None) -> dict:
    """
    Build a single result entry matching the spec schema.
    status   must be FAIL | WARN | ERROR (never CRITICAL — that is a severity level).
    severity may be passed explicitly; otherwise derived from status + check_type.
    column_name is converted to its original JSON path notation (e.g.
      'fact_confidence' → 'extracted_facts[*].confidence').
    """
    entry = {
        "check_id": check_id,
        "column_name": _to_json_path(col),
        "check_type": check_type,
        "status": status,
        "actual_value": actual,
        "expected": expected,
        "severity": severity if severity is not None else _severity(status, check_type),
        "records_failing": records_failing,
        "message": message,
    }
    if sample_failing:
        entry["sample_failing"] = sample_failing[:5]
    return entry


# ── Structural checks ─────────────────────────────────────────────────────────

def check_required(df: pd.DataFrame, properties: dict,
                   contract_id: str, results: list) -> None:
    """CRITICAL: required fields must have zero nulls."""
    for col, clause in properties.items():
        if not clause.get("required", False):
            continue
        cid = _check_id(contract_id, col, "required_field_present")
        if col not in df.columns:
            results.append(_make_result(
                cid, col, "required_field_present", "ERROR",
                actual="column absent",
                expected="column present with zero nulls",
                message=f"Field '{col}' is required but absent from data entirely.",
                records_failing=len(df),
            ))
            continue
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            results.append(_make_result(
                cid, col, "required_field_present", "FAIL",
                actual=f"{null_count} nulls ({null_count/len(df)*100:.1f}%)",
                expected="null_count = 0",
                message=f"Required field has {null_count} null values.",
                records_failing=null_count,
            ))


def check_types(df: pd.DataFrame, properties: dict,
                contract_id: str, results: list) -> None:
    """CRITICAL: numeric contract fields must have numeric dtype."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        expected_type = clause.get("type")
        if expected_type not in ("number", "integer"):
            continue
        cid = _check_id(contract_id, col, "type_match")
        if not pd.api.types.is_numeric_dtype(df[col]):
            sample = [str(v) for v in df[col].dropna().unique()[:5]]
            results.append(_make_result(
                cid, col, "type_match", "FAIL",
                actual=f"dtype={df[col].dtype}",
                expected=f"numeric dtype for type={expected_type}",
                message=f"Contract type is '{expected_type}' but column dtype is '{df[col].dtype}'.",
                records_failing=len(df),
                sample_failing=sample,
            ))


def check_enum(df: pd.DataFrame, properties: dict,
               contract_id: str, results: list) -> None:
    """HIGH: values must be within declared enum set."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        enum_values = clause.get("enum")
        if not enum_values:
            continue
        cid = _check_id(contract_id, col, "enum_conformance")
        non_null = df[col].dropna()
        bad = non_null[~non_null.astype(str).isin(enum_values)]
        if len(bad) > 0:
            results.append(_make_result(
                cid, col, "enum_conformance", "FAIL",
                actual=f"{len(bad)} values not in enum",
                expected=f"values in {enum_values}",
                message=f"{len(bad)} value(s) not in enum {enum_values}.",
                records_failing=len(bad),
                sample_failing=[str(v) for v in bad.unique()[:5]],
            ))


def check_uuid_pattern(df: pd.DataFrame, properties: dict,
                       contract_id: str, results: list) -> None:
    """HIGH: UUID-formatted fields must match UUID pattern."""
    UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        if clause.get("format") != "uuid":
            continue
        cid = _check_id(contract_id, col, "uuid_pattern")
        sample_size = min(100, len(df))
        non_null = df[col].dropna()
        sample_df = non_null.sample(n=min(sample_size, len(non_null)), random_state=42)
        bad = sample_df[~sample_df.astype(str).apply(lambda v: bool(UUID_RE.match(v)))]
        if len(bad) > 0:
            results.append(_make_result(
                cid, col, "uuid_pattern", "FAIL",
                actual=f"{len(bad)} values fail UUID pattern in sample",
                expected="all values match ^[0-9a-f]{8}-...-[0-9a-f]{12}$",
                message=f"{len(bad)} value(s) in sample do not match UUID pattern.",
                records_failing=len(bad),
                sample_failing=[str(v) for v in bad[:5]],
            ))


def check_datetime_format(df: pd.DataFrame, properties: dict,
                          contract_id: str, results: list) -> None:
    """HIGH: date-time fields must be ISO 8601 parseable."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        if clause.get("format") != "date-time":
            continue
        cid = _check_id(contract_id, col, "datetime_format")
        bad_count = 0
        samples = []
        for v in df[col].dropna():
            try:
                datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except ValueError:
                bad_count += 1
                if len(samples) < 5:
                    samples.append(str(v))
        if bad_count > 0:
            results.append(_make_result(
                cid, col, "datetime_format", "FAIL",
                actual=f"{bad_count} values not ISO 8601",
                expected="all values parseable as ISO 8601 date-time",
                message=f"{bad_count} value(s) are not valid ISO 8601 date-times.",
                records_failing=bad_count,
                sample_failing=samples,
            ))


# ── Statistical checks ────────────────────────────────────────────────────────

def check_ranges(df: pd.DataFrame, properties: dict,
                 contract_id: str, results: list) -> None:
    """CRITICAL: numeric columns must stay within contract min/max bounds."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        minimum = clause.get("minimum")
        maximum = clause.get("maximum")
        cid = _check_id(contract_id, col, "range")

        data_min = float(non_null.min())
        data_max = float(non_null.max())

        if minimum is not None and data_min < float(minimum):
            bad_series = non_null[non_null < float(minimum)]
            results.append(_make_result(
                cid, col, "range", "FAIL",
                actual=f"min={data_min}",
                expected=f"min>={minimum}",
                message=f"Data min ({data_min}) is below contract minimum ({minimum}).",
                records_failing=len(bad_series),
                sample_failing=_sample_failing_ids(df, col, bad_series),
            ))

        if maximum is not None and data_max > float(maximum):
            bad_series = non_null[non_null > float(maximum)]
            results.append(_make_result(
                cid, col, "range", "FAIL",
                actual=f"max={data_max}, mean={float(non_null.mean()):.4f}",
                expected=f"max<={maximum}, min>={minimum if minimum is not None else '-inf'}",
                message=(
                    f"Data max ({data_max}) exceeds contract maximum ({maximum})."
                    + (" confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."
                       if "confidence" in col else "")
                ),
                records_failing=len(bad_series),
                sample_failing=_sample_failing_ids(df, col, bad_series),
            ))


def check_statistical_drift(df: pd.DataFrame, properties: dict,
                             contract_id: str, baselines: dict,
                             results: list) -> dict:
    """
    WARN/FAIL: numeric column means must not drift beyond z-score thresholds.
    Returns new baseline entries for previously-unseen columns.
    """
    new_baselines = {}
    for col in df.select_dtypes(include="number").columns:
        non_null = df[col].dropna()
        if len(non_null) < 2:
            continue
        current_mean = float(non_null.mean())
        current_std = float(non_null.std())
        cid = _check_id(contract_id, col, "statistical_drift")

        if col not in baselines:
            new_baselines[col] = {"mean": current_mean, "stddev": current_std}
            continue

        b = baselines[col]
        z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)

        if z_score > 3:
            results.append(_make_result(
                cid, col, "statistical_drift", "FAIL",
                actual=f"mean={current_mean:.4f}, z_score={z_score:.1f}",
                expected=f"z_score<=3.0 (baseline mean={b['mean']:.4f}, stddev={b['stddev']:.4f})",
                message=(
                    f"Mean drifted {z_score:.1f} stddev from baseline "
                    f"(baseline={b['mean']:.4f}, current={current_mean:.4f})."
                ),
                records_failing=len(non_null),
            ))
        elif z_score > 2:
            results.append(_make_result(
                cid, col, "statistical_drift", "WARN",
                actual=f"mean={current_mean:.4f}, z_score={z_score:.1f}",
                expected=f"z_score<=2.0 (baseline mean={b['mean']:.4f}, stddev={b['stddev']:.4f})",
                message=(
                    f"Mean in warning range: {z_score:.1f} stddev from baseline "
                    f"(baseline={b['mean']:.4f}, current={current_mean:.4f})."
                ),
                records_failing=0,
            ))

    return new_baselines


# ── Baseline persistence ──────────────────────────────────────────────────────

def load_baselines(baselines_path: str) -> dict:
    p = Path(baselines_path)
    if p.exists():
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("columns", {})
    return {}


def write_baselines(baselines_path: str, columns: dict) -> None:
    Path(baselines_path).parent.mkdir(parents=True, exist_ok=True)
    with open(baselines_path, "w", encoding="utf-8") as f:
        json.dump({"written_at": _now(), "columns": columns}, f, indent=2)


# ── Check counter ─────────────────────────────────────────────────────────────

def _count_expected_checks(df: pd.DataFrame, properties: dict) -> int:
    """
    Count the number of checks that will be attempted (pass or fail).
    One check per column per applicable check type, plus one drift check per
    numeric column.
    """
    total = 0
    for col, clause in properties.items():
        if clause.get("required", False):
            total += 1  # required_field_present
        expected_type = clause.get("type")
        if expected_type in ("number", "integer"):
            total += 1  # type_match
        if clause.get("enum"):
            total += 1  # enum_conformance
        if clause.get("format") == "uuid":
            total += 1  # uuid_pattern
        if clause.get("format") == "date-time":
            total += 1  # datetime_format
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            if "minimum" in clause:
                total += 1  # range min check
            if "maximum" in clause:
                total += 1  # range max check
    # One statistical_drift check per numeric column in the data
    total += len(df.select_dtypes(include="number").columns)
    return total


# ── Report assembly ───────────────────────────────────────────────────────────

def _overall_status(results: list[dict]) -> str:
    statuses = {r["status"] for r in results}
    if "ERROR" in statuses:
        return "FAIL"
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"


def run_validation(
    contract_path: str,
    data_path: str,
    output_path: str,
    baselines_path: str = "schema_snapshots/baselines.json",
    mode: str = "AUDIT",
) -> dict:
    """
    Full validation pipeline. Returns the report dict and writes it to output_path.
    Report schema matches Phase 2 spec exactly.
    """
    # Load contract
    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    contract_id = contract.get("id", "unknown")
    contract_version = (
        contract.get("info", {}).get("version")
        or contract.get("version", "unknown")
    )
    properties: dict = contract.get("schema", {}).get("properties", {})

    # Load and flatten data
    records = load_jsonl(data_path)
    df = flatten_for_profile(records)

    results: list[dict] = []

    # Run all checks — never crash, always append ERROR result on exception
    check_fns = [
        ("required",        lambda: check_required(df, properties, contract_id, results)),
        ("types",           lambda: check_types(df, properties, contract_id, results)),
        ("enum",            lambda: check_enum(df, properties, contract_id, results)),
        ("uuid_pattern",    lambda: check_uuid_pattern(df, properties, contract_id, results)),
        ("datetime_format", lambda: check_datetime_format(df, properties, contract_id, results)),
        ("range",           lambda: check_ranges(df, properties, contract_id, results)),
    ]
    for name, fn in check_fns:
        try:
            fn()
        except Exception as e:
            results.append(_make_result(
                f"{contract_id}.check.{name}", name, name, "ERROR",
                actual="exception", expected="clean execution",
                message=f"Check '{name}' raised an exception: {e}",
            ))

    # Statistical drift
    baselines = load_baselines(baselines_path)
    try:
        new_baselines = check_statistical_drift(
            df, properties, contract_id, baselines, results
        )
    except Exception as e:
        new_baselines = {}
        results.append(_make_result(
            f"{contract_id}.check.statistical_drift", "all_numeric", "statistical_drift",
            "ERROR", actual="exception", expected="clean execution",
            message=f"Statistical drift check raised an exception: {e}",
        ))

    if new_baselines:
        merged = {**baselines, **new_baselines}
        write_baselines(baselines_path, merged)

    overall = _overall_status(results)

    # Count checks by status (results only contains non-passing entries)
    status_counts: dict[str, int] = {"FAIL": 0, "WARN": 0, "ERROR": 0}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    # total_checks includes passing checks (not in results list)
    total_checks = _count_expected_checks(df, properties)
    non_passing = status_counts.get("FAIL", 0) + status_counts.get("WARN", 0) + status_counts.get("ERROR", 0)
    passed = max(0, total_checks - non_passing)

    report = {
        "report_id": str(_uuid_mod.uuid4()),
        "contract_id": contract_id,
        "contract_version": contract_version,
        "snapshot_id": _sha256_file(data_path),
        "run_timestamp": _now(),
        "data_path": data_path,
        "record_count": len(records),
        "enforcement_mode": mode,
        "total_checks": total_checks,
        "passed": passed,
        "failed": status_counts.get("FAIL", 0),
        "warned": status_counts.get("WARN", 0),
        "errored": status_counts.get("ERROR", 0),
        "overall_status": overall,
        "violation_count": status_counts.get("FAIL", 0),
        "results": results,
        # Legacy field kept for backward compatibility with existing tests/dashboard
        "violations": [
            {
                "status": r["status"],
                "severity": r["severity"],
                "check": r["check_type"],
                "field": r["column_name"],
                "reason": r["message"],
                **({"sample": r["sample_failing"]} if "sample_failing" in r else {}),
            }
            for r in results
            if r["status"] in ("FAIL", "WARN", "ERROR")
        ],
        "summary": {
            "FAIL": status_counts.get("FAIL", 0),
            "WARN": status_counts.get("WARN", 0),
            "ERROR": status_counts.get("ERROR", 0),
        },
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    import sys

    parser = argparse.ArgumentParser(
        description="Validate JSONL data against a Bitol YAML contract."
    )
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--baselines", default="schema_snapshots/baselines.json")
    parser.add_argument(
        "--mode",
        choices=["AUDIT", "WARN", "ENFORCE"],
        default="AUDIT",
        help=(
            "AUDIT: log only, never block (default). "
            "WARN: exit 1 on CRITICAL severity violations. "
            "ENFORCE: exit 1 on CRITICAL or HIGH severity violations."
        ),
    )
    args = parser.parse_args()

    report = run_validation(
        contract_path=args.contract,
        data_path=args.data,
        output_path=args.output,
        baselines_path=args.baselines,
        mode=args.mode,
    )

    print(f"\n[runner] Mode           : {args.mode}")
    print(f"[runner] Overall status : {report['overall_status']}")
    print(f"[runner] Records checked: {report['record_count']}")
    print(f"[runner] Checks run     : {report['total_checks']} "
          f"(passed={report['passed']}, failed={report['failed']}, "
          f"warned={report['warned']}, errored={report['errored']})")
    print(f"[runner] Report written : {args.output}")

    if report["violations"]:
        print("\n[runner] Violation details:")
        for v in report["violations"]:
            print(f"  [{v['status']}|{v['severity']}] {v['check']} / {v['field']}: {v['reason']}")

    # ── Mode-based exit code ──────────────────────────────────────────────────
    # AUDIT: always succeed — observe without blocking
    # WARN:  block on CRITICAL-severity violations
    # ENFORCE: block on CRITICAL or HIGH severity violations
    if args.mode != "AUDIT":
        blocking_severities = {"CRITICAL"} if args.mode == "WARN" else {"CRITICAL", "HIGH"}
        blocking = [
            r for r in report["results"]
            if r.get("severity") in blocking_severities
        ]
        if blocking:
            print(
                f"\n[runner] {args.mode} mode: {len(blocking)} blocking violation(s) detected. "
                "Exiting with code 1."
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
