"""
ValidationRunner — Phase 2
Validates a JSONL data file against a Bitol contract and produces a structured report.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_$(date +%Y%m%d_%H%M).json
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
    """Produce a dotted check identifier: contract.column.check_type"""
    return f"{contract_id}.{col}.{check_type}"


def _severity(status: str) -> str:
    mapping = {
        "CRITICAL": "CRITICAL",
        "FAIL": "HIGH",
        "WARN": "MEDIUM",
        "ERROR": "LOW",
    }
    return mapping.get(status, "LOW")


def _make_result(check_id: str, col: str, check_type: str, status: str,
                 actual: str, expected: str, message: str,
                 records_failing: int = 0, sample_failing: list = None) -> dict:
    """Build a single result entry matching the spec schema."""
    entry = {
        "check_id": check_id,
        "column_name": col,
        "check_type": check_type,
        "status": status,
        "actual_value": actual,
        "expected": expected,
        "severity": _severity(status),
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
                cid, col, "required_field_present", "CRITICAL",
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
                cid, col, "type_match", "CRITICAL",
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
            bad_rows = int((non_null < float(minimum)).sum())
            results.append(_make_result(
                cid, col, "range", "CRITICAL",
                actual=f"min={data_min}",
                expected=f"min>={minimum}",
                message=f"Data min ({data_min}) is below contract minimum ({minimum}).",
                records_failing=bad_rows,
            ))

        if maximum is not None and data_max > float(maximum):
            bad_rows = int((non_null > float(maximum)).sum())
            results.append(_make_result(
                cid, col, "range", "CRITICAL",
                actual=f"max={data_max}, mean={float(non_null.mean()):.4f}",
                expected=f"max<={maximum}, min>={minimum if minimum is not None else '-inf'}",
                message=(
                    f"Data max ({data_max}) exceeds contract maximum ({maximum})."
                    + (" confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."
                       if "confidence" in col else "")
                ),
                records_failing=bad_rows,
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


# ── Report assembly ───────────────────────────────────────────────────────────

def _overall_status(results: list[dict]) -> str:
    statuses = {r["status"] for r in results}
    if "CRITICAL" in statuses or "ERROR" in statuses:
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

    # Count by status
    status_counts = {"CRITICAL": 0, "FAIL": 0, "WARN": 0, "ERROR": 0, "PASS": 0}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    passed = len(results) - sum(
        v for k, v in status_counts.items() if k != "PASS"
    )

    report = {
        "report_id": str(_uuid_mod.uuid4()),
        "contract_id": contract_id,
        "contract_version": contract_version,
        "snapshot_id": _sha256_file(data_path),
        "run_timestamp": _now(),
        "data_path": data_path,
        "record_count": len(records),
        "total_checks": len(results),
        "passed": max(0, passed),
        "failed": status_counts.get("CRITICAL", 0) + status_counts.get("FAIL", 0),
        "warned": status_counts.get("WARN", 0),
        "errored": status_counts.get("ERROR", 0),
        "overall_status": overall,
        "violation_count": status_counts.get("CRITICAL", 0) + status_counts.get("FAIL", 0),
        "results": results,
        # Legacy field kept for backward compatibility with existing tests/dashboard
        "violations": [
            {
                "status": r["status"],
                "check": r["check_type"],
                "field": r["column_name"],
                "reason": r["message"],
                **({"sample": r["sample_failing"]} if "sample_failing" in r else {}),
            }
            for r in results
            if r["status"] in ("CRITICAL", "FAIL", "WARN", "ERROR")
        ],
        "summary": {
            "CRITICAL": status_counts.get("CRITICAL", 0),
            "FAIL": status_counts.get("FAIL", 0),
            "WARN": status_counts.get("WARN", 0),
        },
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate JSONL data against a Bitol YAML contract."
    )
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--baselines", default="schema_snapshots/baselines.json")
    args = parser.parse_args()

    report = run_validation(
        contract_path=args.contract,
        data_path=args.data,
        output_path=args.output,
        baselines_path=args.baselines,
    )

    print(f"\n[runner] Overall status : {report['overall_status']}")
    print(f"[runner] Records checked: {report['record_count']}")
    print(f"[runner] Checks run     : {report['total_checks']} "
          f"(passed={report['passed']}, failed={report['failed']}, "
          f"warned={report['warned']}, errored={report['errored']})")
    print(f"[runner] Report written : {args.output}")

    if report["violations"]:
        print("\n[runner] Violation details:")
        for v in report["violations"]:
            print(f"  [{v['status']}] {v['check']} / {v['field']}: {v['reason']}")


if __name__ == "__main__":
    main()
