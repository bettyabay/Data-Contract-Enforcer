"""
ValidationRunner — Phase 1
Validates a JSONL data file against a generated Bitol YAML contract.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3-document-refinery-extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_baseline.json
"""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from contracts.generator import load_jsonl, flatten_for_profile


# ── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _emit(violations: list, status: str, check: str, field: str, reason: str,
          sample=None) -> dict:
    entry = {
        "status": status,
        "check": check,
        "field": field,
        "reason": reason,
    }
    if sample is not None:
        entry["sample"] = sample
    violations.append(entry)
    return entry


# ── Structural checks ────────────────────────────────────────────────────────

def check_required(df: pd.DataFrame, properties: dict, violations: list) -> None:
    """CRITICAL: required fields must have zero nulls."""
    for col, clause in properties.items():
        if not clause.get("required", False):
            continue
        if col not in df.columns:
            _emit(violations, "CRITICAL", "required_field_present", col,
                  f"Field '{col}' is required but absent from data entirely.")
            continue
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            _emit(violations, "CRITICAL", "required_field_present", col,
                  f"Required field has {null_count} null values "
                  f"({null_count/len(df)*100:.1f}% of rows).")


def check_types(df: pd.DataFrame, properties: dict, violations: list) -> None:
    """CRITICAL: numeric contract fields must have numeric dtype in data."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        expected = clause.get("type")
        if expected in ("number", "integer"):
            if not pd.api.types.is_numeric_dtype(df[col]):
                sample = [str(v) for v in df[col].dropna().unique()[:5]]
                _emit(violations, "CRITICAL", "type_match", col,
                      f"Contract type is '{expected}' but column dtype is "
                      f"'{df[col].dtype}'.",
                      sample=sample)


def check_enum(df: pd.DataFrame, properties: dict, violations: list) -> None:
    """FAIL: values must be within declared enum set."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        enum_values = clause.get("enum")
        if not enum_values:
            continue
        non_null = df[col].dropna()
        bad = non_null[~non_null.astype(str).isin(enum_values)]
        if len(bad) > 0:
            _emit(violations, "FAIL", "enum_conformance", col,
                  f"{len(bad)} value(s) not in enum {enum_values}.",
                  sample=list(bad.unique()[:5]))


def check_uuid_pattern(df: pd.DataFrame, properties: dict, violations: list) -> None:
    """FAIL: UUID-formatted fields must match uuid pattern."""
    UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        if clause.get("format") != "uuid":
            continue
        sample_size = min(100, len(df))
        sample_df = df[col].dropna().sample(n=min(sample_size, len(df[col].dropna())),
                                             random_state=42)
        bad = sample_df[~sample_df.astype(str).apply(lambda v: bool(UUID_RE.match(v)))]
        if len(bad) > 0:
            _emit(violations, "FAIL", "uuid_pattern", col,
                  f"{len(bad)} value(s) in sample do not match UUID pattern.",
                  sample=list(bad[:5]))


def check_datetime_format(df: pd.DataFrame, properties: dict, violations: list) -> None:
    """FAIL: date-time fields must be ISO 8601 parseable."""
    for col, clause in properties.items():
        if col not in df.columns:
            continue
        if clause.get("format") != "date-time":
            continue
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
            _emit(violations, "FAIL", "datetime_format", col,
                  f"{bad_count} value(s) are not valid ISO 8601 date-times.",
                  sample=samples)


# ── Statistical checks ────────────────────────────────────────────────────────

def check_ranges(df: pd.DataFrame, properties: dict, violations: list) -> None:
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

        if minimum is not None and float(non_null.min()) < float(minimum):
            _emit(violations, "CRITICAL", "range", col,
                  f"Data min ({non_null.min()}) is below contract minimum ({minimum}).")

        if maximum is not None and float(non_null.max()) > float(maximum):
            _emit(violations, "CRITICAL", "range", col,
                  f"Data max ({non_null.max()}) exceeds contract maximum ({maximum}).")


def check_statistical_drift(df: pd.DataFrame, properties: dict,
                             baselines: dict, violations: list) -> list[dict]:
    """
    WARN/FAIL: numeric column means must not drift beyond z-score thresholds
    relative to stored baselines.
    Returns updated baseline entries for columns with no prior baseline.
    """
    new_baselines = {}
    for col in df.select_dtypes(include="number").columns:
        non_null = df[col].dropna()
        if len(non_null) < 2:
            continue
        current_mean = float(non_null.mean())
        current_std = float(non_null.std())

        if col not in baselines:
            new_baselines[col] = {"mean": current_mean, "stddev": current_std}
            continue

        b = baselines[col]
        z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)

        if z_score > 3:
            _emit(violations, "FAIL", "statistical_drift", col,
                  f"Mean drifted {z_score:.1f} stddev from baseline "
                  f"(baseline={b['mean']:.4f}, current={current_mean:.4f}).")
        elif z_score > 2:
            _emit(violations, "WARN", "statistical_drift", col,
                  f"Mean in warning range: {z_score:.1f} stddev from baseline "
                  f"(baseline={b['mean']:.4f}, current={current_mean:.4f}).")

    return new_baselines


# ── Baseline persistence ─────────────────────────────────────────────────────

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
        json.dump(
            {"written_at": _now(), "columns": columns},
            f, indent=2,
        )


# ── Report assembly ──────────────────────────────────────────────────────────

def _overall_status(violations: list[dict]) -> str:
    statuses = {v["status"] for v in violations}
    if "CRITICAL" in statuses:
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
    Full validation pipeline.
    Returns the report dict and writes it to output_path.
    """
    # Load contract
    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    properties: dict = contract.get("schema", {}).get("properties", {})

    # Load and flatten data
    records = load_jsonl(data_path)
    df = flatten_for_profile(records)

    violations: list[dict] = []

    # Structural checks (fail-fast order)
    check_required(df, properties, violations)
    check_types(df, properties, violations)
    check_enum(df, properties, violations)
    check_uuid_pattern(df, properties, violations)
    check_datetime_format(df, properties, violations)

    # Statistical checks
    check_ranges(df, properties, violations)
    baselines = load_baselines(baselines_path)
    new_baselines = check_statistical_drift(df, properties, baselines, violations)

    # Write new baselines for columns not yet baselined
    if new_baselines:
        merged = {**baselines, **new_baselines}
        write_baselines(baselines_path, merged)

    overall = _overall_status(violations)

    report = {
        "contract_id": contract.get("id", "unknown"),
        "contract_version": contract.get("version", "unknown"),
        "data_path": data_path,
        "record_count": len(records),
        "validated_at": _now(),
        "overall_status": overall,
        "violation_count": len(violations),
        "violations": violations,
        "summary": {
            "CRITICAL": sum(1 for v in violations if v["status"] == "CRITICAL"),
            "FAIL": sum(1 for v in violations if v["status"] == "FAIL"),
            "WARN": sum(1 for v in violations if v["status"] == "WARN"),
        },
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report


# ── CLI entry point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate JSONL data against a Bitol YAML contract."
    )
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to data JSONL file")
    parser.add_argument("--output", required=True, help="Path to write validation report JSON")
    parser.add_argument(
        "--baselines",
        default="schema_snapshots/baselines.json",
        help="Path to statistical baselines JSON",
    )
    args = parser.parse_args()

    report = run_validation(
        contract_path=args.contract,
        data_path=args.data,
        output_path=args.output,
        baselines_path=args.baselines,
    )

    print(f"\n[runner] Overall status : {report['overall_status']}")
    print(f"[runner] Records checked: {report['record_count']}")
    print(f"[runner] Violations     : {report['violation_count']} "
          f"(CRITICAL={report['summary']['CRITICAL']}, "
          f"FAIL={report['summary']['FAIL']}, "
          f"WARN={report['summary']['WARN']})")
    print(f"[runner] Report written : {args.output}")

    if report["violations"]:
        print("\n[runner] Violation details:")
        for v in report["violations"]:
            print(f"  [{v['status']}] {v['check']} / {v['field']}: {v['reason']}")


if __name__ == "__main__":
    main()
