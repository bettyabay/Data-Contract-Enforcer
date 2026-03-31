"""
ContractGenerator — Phase 1
Generates a Bitol-style YAML data contract from a JSONL source file.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


# ── Stage 1: Load and flatten ────────────────────────────────────────────────

def load_jsonl(path: str) -> list[dict]:
    """Load a JSONL file, skipping blank lines."""
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """
    Flatten nested JSONL to a DataFrame suitable for column profiling.

    Strategy:
    - Scalar top-level fields become columns directly.
    - Known array fields (extracted_facts, entities, code_refs, scores,
      nodes, edges) are exploded so each item becomes its own row with
      a prefixed column name.
    - Unknown nested dicts are flattened with dot notation.
    - If no array fields are found, return a flat DataFrame.
    """
    ARRAY_FIELDS = {
        "extracted_facts": "fact_",
        "entities": "entity_",
        "code_refs": "ref_",
        "nodes": "node_",
        "edges": "edge_",
    }

    rows = []
    for r in records:
        base = {}
        arrays = {}
        for k, v in r.items():
            if isinstance(v, list) and k in ARRAY_FIELDS:
                arrays[k] = v
            elif isinstance(v, dict):
                for dk, dv in v.items():
                    if not isinstance(dv, (dict, list)):
                        base[f"{k}_{dk}"] = dv
            elif not isinstance(v, list):
                base[k] = v

        if arrays:
            # Explode the first found array field
            field, prefix = next(
                ((k, ARRAY_FIELDS[k]) for k in ARRAY_FIELDS if k in arrays),
                (None, None)
            )
            if field:
                items = arrays[field] if arrays[field] else [{}]
                for item in items:
                    row = dict(base)
                    if isinstance(item, dict):
                        for ik, iv in item.items():
                            if not isinstance(iv, (dict, list)):
                                row[f"{prefix}{ik}"] = iv
                    rows.append(row)
            else:
                rows.append(base)
        else:
            rows.append(base)

    return pd.DataFrame(rows)


# ── Stage 2: Column profiling ────────────────────────────────────────────────

def profile_column(series: pd.Series, col_name: str) -> dict:
    """Produce a statistical profile for a single column."""
    profile = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().unique()[:5]],
    }
    if pd.api.types.is_numeric_dtype(series):
        non_null = series.dropna()
        if len(non_null) > 0:
            profile["stats"] = {
                "min": float(non_null.min()),
                "max": float(non_null.max()),
                "mean": float(non_null.mean()),
                "p25": float(non_null.quantile(0.25)),
                "p50": float(non_null.quantile(0.50)),
                "p75": float(non_null.quantile(0.75)),
                "p95": float(non_null.quantile(0.95)),
                "p99": float(non_null.quantile(0.99)),
                "stddev": float(non_null.std()),
            }
    return profile


def profile_all_columns(df: pd.DataFrame) -> dict[str, dict]:
    return {col: profile_column(df[col], col) for col in df.columns}


# ── Stage 3: Profile → Bitol YAML clause ────────────────────────────────────

def infer_type(dtype_str: str) -> str:
    mapping = {
        "float64": "number",
        "int64": "integer",
        "int32": "integer",
        "bool": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


def column_to_clause(profile: dict) -> dict:
    """Translate a column profile into a Bitol YAML property clause."""
    col = profile["name"]
    dtype_str = profile["dtype"]
    json_type = infer_type(dtype_str)

    clause: dict = {
        "type": json_type,
        "required": profile["null_fraction"] == 0.0,
    }

    # Confidence fields — enforce 0.0–1.0 range
    if "confidence" in col and json_type == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score. MUST remain 0.0-1.0 float. "
            "BREAKING if changed to 0-100 percentage scale."
        )

    # Range from stats for other numeric fields
    elif json_type in ("number", "integer") and "stats" in profile:
        stats = profile["stats"]
        # Use p1/p99 as soft bounds (accommodate outliers)
        clause["minimum"] = round(stats["min"], 6)
        clause["maximum"] = round(stats["max"], 6)

    # UUID fields
    if col.endswith("_id"):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

    # Date-time fields
    if col.endswith("_at"):
        clause["format"] = "date-time"

    # Low-cardinality string fields → enum
    if (
        json_type == "string"
        and profile["cardinality_estimate"] <= 10
        and profile["null_fraction"] < 1.0
        and len(profile["sample_values"]) >= profile["cardinality_estimate"]
    ):
        clause["enum"] = sorted(profile["sample_values"])

    # Positive-only fields
    if col in ("processing_time_ms", "latency_ms", "total_tokens",
               "prompt_tokens", "completion_tokens", "token_count_input",
               "token_count_output"):
        clause["minimum"] = 1
        clause["type"] = "integer"

    return clause


# ── Stage 4: Lineage injection and contract assembly ─────────────────────────

def inject_lineage(contract: dict, lineage_path: str | None, contract_id: str) -> dict:
    """Inject lineage metadata from the latest lineage snapshot."""
    if not lineage_path or not Path(lineage_path).exists():
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract

    with open(lineage_path, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    snapshot = json.loads(lines[-1])

    keyword = contract_id.split("-")[0]  # e.g. "week3"
    consumers = [
        e["target"]
        for e in snapshot.get("edges", [])
        if keyword in e.get("source", "") or keyword in e.get("target", "")
    ]
    producers = [
        e["source"]
        for e in snapshot.get("edges", [])
        if keyword in e.get("target", "")
    ]

    contract["lineage"] = {
        "upstream": [{"id": p} for p in set(producers)],
        "downstream": [
            {"id": c, "fields_consumed": _infer_consumed_fields(contract_id)}
            for c in set(consumers)
        ],
    }
    return contract


def _infer_consumed_fields(contract_id: str) -> list[str]:
    """Return the primary key fields likely consumed by downstream systems."""
    field_map = {
        "week1": ["intent_id", "code_refs", "confidence"],
        "week2": ["verdict_id", "overall_verdict", "overall_score"],
        "week3": ["doc_id", "extracted_facts", "entities"],
        "week4": ["snapshot_id", "nodes", "edges"],
        "week5": ["event_id", "event_type", "payload"],
    }
    for k, fields in field_map.items():
        if k in contract_id:
            return fields
    return ["id"]


def build_contract(
    records: list[dict],
    df: pd.DataFrame,
    column_profiles: dict[str, dict],
    contract_id: str,
    source_path: str,
) -> dict:
    """Assemble the full Bitol YAML contract dict."""
    source_hash = _hash_file(source_path)

    properties = {
        col: column_to_clause(profile)
        for col, profile in column_profiles.items()
        if not col.startswith("Unnamed")
    }

    return {
        "apiVersion": "v2.2.2",
        "kind": "DataContract",
        "id": contract_id,
        "version": "1.0.0",
        "description": f"Auto-generated contract for {source_path}",
        "source": {
            "path": source_path,
            "sha256": source_hash,
            "record_count": len(records),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "schema": {
            "type": "object",
            "properties": properties,
        },
        "quality": {
            "checks": _build_quality_checks(column_profiles),
        },
        "lineage": {"upstream": [], "downstream": []},
    }


def _build_quality_checks(column_profiles: dict[str, dict]) -> list[dict]:
    checks = []
    for col, profile in column_profiles.items():
        if "confidence" in col and profile["dtype"] in ("float64", "object"):
            checks.append({
                "type": "range",
                "field": col,
                "minimum": 0.0,
                "maximum": 1.0,
                "severity": "CRITICAL",
            })
        if col.endswith("_at"):
            checks.append({
                "type": "format",
                "field": col,
                "format": "date-time",
                "severity": "CRITICAL",
            })
        if pd.api.types.is_numeric_dtype(pd.Series(dtype=profile["dtype"])) and "stats" in profile:
            checks.append({
                "type": "statistical_drift",
                "field": col,
                "z_score_warn": 2.0,
                "z_score_fail": 3.0,
            })
    return checks


def _hash_file(path: str) -> str:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except FileNotFoundError:
        h.update(path.encode())
    return h.hexdigest()


# ── CLI entry point ──────────────────────────────────────────────────────────

def generate(source: str, contract_id: str, lineage: str | None, output_dir: str) -> Path:
    """
    Full pipeline: load → flatten → profile → contract → write YAML.
    Returns the path to the written contract file.
    """
    print(f"[generator] Loading {source}...")
    records = load_jsonl(source)
    print(f"[generator] Loaded {len(records)} records.")

    df = flatten_for_profile(records)
    print(f"[generator] Flattened to DataFrame: {df.shape[0]} rows x {df.shape[1]} cols")

    # Warn if confidence column appears as object dtype (mixed types)
    for col in df.columns:
        if "confidence" in col and df[col].dtype == object:
            print(
                f"[generator] WARNING: column '{col}' has dtype=object "
                "(expected float64). Mixed types detected — document in DOMAIN_NOTES.md."
            )

    column_profiles = profile_all_columns(df)

    contract = build_contract(records, df, column_profiles, contract_id, source)
    contract = inject_lineage(contract, lineage, contract_id)

    out_path = Path(output_dir) / f"{contract_id}.yaml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"[generator] Contract written to {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Generate a data contract from a JSONL source.")
    parser.add_argument("--source", required=True, help="Path to source JSONL file")
    parser.add_argument("--contract-id", required=True, help="Unique contract identifier")
    parser.add_argument("--lineage", default=None, help="Path to lineage snapshots JSONL")
    parser.add_argument("--output", default="generated_contracts/", help="Output directory")
    args = parser.parse_args()

    generate(
        source=args.source,
        contract_id=args.contract_id,
        lineage=args.lineage,
        output_dir=args.output,
    )


if __name__ == "__main__":
    main()
