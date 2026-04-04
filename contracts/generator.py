"""
ContractGenerator — Phase 1
Generates a Bitol v3.0.0 YAML data contract from a JSONL source file.
Also produces a parallel dbt schema.yml for every contract generated.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3_extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""

import argparse
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# Load .env if python-dotenv is available (silently skip if not installed)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from ydata_profiling import ProfileReport
    YDATA_AVAILABLE = True
except ImportError:
    YDATA_AVAILABLE = False


# ── Constants ────────────────────────────────────────────────────────────────

# Mapping from contract keyword → human-readable info block values
_INFO_MAP = {
    "week1": {
        "title": "Week 1 Intent Correlator — Intent Records",
        "owner": "week1-team",
        "description": "One record per user query. Each record contains the classified intent and all code references found.",
    },
    "week3": {
        "title": "Week 3 Document Refinery — Extraction Records",
        "owner": "week3-team",
        "description": "One record per processed document. Each record contains all facts extracted from the source document and the entities referenced.",
    },
    "week4": {
        "title": "Week 4 Code Cartographer — Lineage Snapshots",
        "owner": "week4-team",
        "description": "One record per lineage snapshot. Each snapshot is a point-in-time graph of nodes and edges representing data dependencies.",
    },
    "week5": {
        "title": "Week 5 Event Sourcing Platform — Events",
        "owner": "week5-team",
        "description": "One record per system event. Events are immutable, ordered by sequence_number, and carry a typed payload.",
    },
    "langsmith": {
        "title": "LangSmith Agent Traces",
        "owner": "ai-team",
        "description": "One record per LangSmith trace run. Covers latency, token usage, error rate, and AI-reported confidence scores.",
    },
}

# Fields that, if changed, break the downstream consumers of each contract
_BREAKING_FIELDS_MAP = {
    "week1": ["ref_confidence", "intent_id"],
    "week3": ["fact_confidence", "doc_id"],
    "week4": ["snapshot_id", "nodes", "edges"],
    "week5": ["event_id", "sequence_number"],
    "langsmith": ["outputs_confidence", "run_id", "latency_ms"],
}


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


def _ydata_structural_profile(df: pd.DataFrame) -> dict[str, dict]:
    """
    Run ydata-profiling in minimal mode to extract dominant character patterns
    for string columns. Returns {col_name: {"dominant_pattern": str}}.
    Falls back to empty dict if ydata-profiling is unavailable or fails.
    """
    if not YDATA_AVAILABLE:
        return {}
    try:
        report = ProfileReport(df, minimal=True, progress_bar=False)
        desc = report.get_description()
        result = {}
        for col, var in desc.variables.items():
            entry = {}
            # Extract dominant pattern for string/categorical columns
            pat = getattr(var, "pattern_counts", None)
            if pat and len(pat) > 0:
                dominant = max(pat, key=pat.get)
                entry["dominant_pattern"] = dominant
            if entry:
                result[col] = entry
        return result
    except Exception:
        return {}


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
        clause["minimum"] = round(stats["min"], 6)
        clause["maximum"] = round(stats["max"], 6)

    # UUID fields — only tag as uuid when sample values actually match UUID format
    _UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    if col.endswith("_id") and json_type == "string":
        samples = [v for v in profile.get("sample_values", []) if isinstance(v, str)]
        if samples and all(_UUID_RE.match(v) for v in samples):
            clause["format"] = "uuid"
            clause["pattern"] = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            clause["unique"] = True

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

    # Dominant character pattern from ydata-profiling
    if "dominant_pattern" in profile:
        clause["dominant_pattern"] = profile["dominant_pattern"]

    return clause


# ── Stage 4: Lineage injection and contract assembly ─────────────────────────

def inject_lineage(
    contract: dict,
    lineage_path: str | None,
    contract_id: str,
    registry_path: str | None = None,
) -> dict:
    """
    Inject lineage metadata into the contract.

    Primary source  — ContractRegistry (subscriptions.yaml): authoritative list of
                      downstream subscribers and which fields they depend on.
    Enrichment source — Week 4 lineage graph: upstream producers and any additional
                        consumers not yet registered.

    Fallback: if no registry is provided, use the hardcoded field maps as before.
    """
    # ── Primary: registry subscribers ────────────────────────────────────────
    registry_downstream: list[dict] = []
    if registry_path and Path(registry_path).exists():
        with open(registry_path, encoding="utf-8") as f:
            reg_data = yaml.safe_load(f) or {}
        for sub in reg_data.get("subscriptions", []):
            if sub.get("contract_id") != contract_id:
                continue
            breaking_fields = sub.get("breaking_fields", [])
            field_names = [
                bf["field"] if isinstance(bf, dict) else str(bf)
                for bf in breaking_fields
            ]
            registry_downstream.append({
                "id": sub.get("subscriber_id"),
                "description": (
                    f"Registered subscriber via ContractRegistry. "
                    f"Validation mode: {sub.get('validation_mode', 'AUDIT')}."
                ),
                "fields_consumed": sub.get("fields_consumed", []),
                "breaking_if_changed": field_names,
                "validation_mode": sub.get("validation_mode"),
                "contact": sub.get("contact"),
            })

    # ── Enrichment: lineage graph for upstream producers ──────────────────────
    upstream: list[dict] = []
    if lineage_path and Path(lineage_path).exists():
        with open(lineage_path, encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        if lines:
            snapshot = json.loads(lines[-1])
            keyword = contract_id.split("-")[0].split("_")[0]  # e.g. "week3"
            producers = [
                e["source"]
                for e in snapshot.get("edges", [])
                if keyword in e.get("target", "")
            ]
            upstream = [{"id": p} for p in set(producers)]

            # If registry had no subscriptions for this contract, fall back to
            # hardcoded inference from the lineage graph so the lineage block
            # is never empty.
            if not registry_downstream:
                breaking = _infer_breaking_fields(contract_id)
                consumed = _infer_consumed_fields(contract_id)
                consumers = [
                    e["target"]
                    for e in snapshot.get("edges", [])
                    if keyword in e.get("source", "") or keyword in e.get("target", "")
                ]
                registry_downstream = [
                    {
                        "id": c,
                        "fields_consumed": consumed,
                        "breaking_if_changed": breaking,
                    }
                    for c in set(consumers)
                ]

    contract["lineage"] = {
        "upstream": upstream,
        "downstream": registry_downstream,
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
        "langsmith": ["run_id", "latency_ms", "total_tokens", "error", "outputs"],
    }
    for k, fields in field_map.items():
        if k in contract_id:
            return fields
    return ["id"]


def _infer_breaking_fields(contract_id: str) -> list[str]:
    """Return fields that are breaking if changed for the given contract."""
    for k, fields in _BREAKING_FIELDS_MAP.items():
        if k in contract_id:
            return fields
    return []


def _get_info(contract_id: str) -> dict:
    """Return the info block for a given contract_id."""
    for keyword, info in _INFO_MAP.items():
        if keyword in contract_id:
            return info
    # Fallback: derive from contract_id
    title = contract_id.replace("_", " ").replace("-", " ").title()
    return {
        "title": title,
        "owner": "unknown",
        "description": f"Auto-generated contract for {contract_id}.",
    }


def _table_name(contract_id: str) -> str:
    """Derive a clean table name from contract_id."""
    return contract_id.replace("-", "_")


def build_contract(
    records: list[dict],
    df: pd.DataFrame,
    column_profiles: dict[str, dict],
    contract_id: str,
    source_path: str,
) -> dict:
    """Assemble the full Bitol v3.0.0 contract dict."""
    source_hash = _hash_file(source_path)
    info = _get_info(contract_id)
    tname = _table_name(contract_id)

    properties = {
        col: column_to_clause(profile)
        for col, profile in column_profiles.items()
        if not col.startswith("Unnamed")
    }

    # Detect if any confidence field exists for the terms block
    has_confidence = any("confidence" in col for col in properties)

    return {
        "apiVersion": "v3.0.0",
        "kind": "DataContract",
        "id": contract_id,
        "info": {
            "title": info["title"],
            "version": "1.0.0",
            "owner": info["owner"],
            "description": info["description"],
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": (
                "confidence must remain in 0.0-1.0 float range."
                if has_confidence
                else "See schema constraints for field-level limitations."
            ),
        },
        # source block kept for backward compatibility with tests and runner
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
        "quality": _build_quality_checks(column_profiles, tname),
        "lineage": {"upstream": [], "downstream": []},
    }


def _build_quality_checks(column_profiles: dict[str, dict], table_name: str = "records") -> dict:
    """
    Build a SodaChecks-format quality block.
    String expressions cover structural checks; drift_checks covers statistical.
    """
    soda_expressions = ["row_count >= 1"]
    drift_checks = []

    for col, profile in column_profiles.items():
        # Structural: required fields must have no nulls
        if profile["null_fraction"] == 0.0:
            soda_expressions.append(f"missing_count({col}) = 0")

        # Structural: ID fields must be unique
        if col.endswith("_id"):
            soda_expressions.append(f"duplicate_count({col}) = 0")

        # Structural: confidence range
        if "confidence" in col and profile["dtype"] in ("float64", "object"):
            soda_expressions.append(f"min({col}) >= 0.0")
            soda_expressions.append(f"max({col}) <= 1.0")

        # Structural: date-time format
        if col.endswith("_at"):
            soda_expressions.append(f"invalid_count({col}) = 0  # format: date-time")

        # Statistical: drift detection on numeric columns
        if pd.api.types.is_numeric_dtype(pd.Series(dtype=profile["dtype"])) and "stats" in profile:
            drift_checks.append({
                "type": "statistical_drift",
                "field": col,
                "z_score_warn": 2.0,
                "z_score_fail": 3.0,
            })

    return {
        "type": "SodaChecks",
        "specification": {
            f"checks for {table_name}": soda_expressions,
        },
        "drift_checks": drift_checks,
    }


def _hash_file(path: str) -> str:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except FileNotFoundError:
        h.update(path.encode())
    return h.hexdigest()


# ── Stage 5: dbt schema.yml output ──────────────────────────────────────────

def generate_dbt_schema(contract: dict, out_path: Path) -> None:
    """
    Produce a dbt schema.yml from a Bitol contract.
    Maps contract clauses to dbt tests:
      required:true  → not_null
      unique:true    → unique
      enum:[...]     → accepted_values
      min/max        → dbt_expectations.expect_column_values_to_be_between
      format:uuid    → dbt_expectations.expect_column_values_to_match_regex
      format:date-time → dbt_utils.expression_is_true (ISO 8601 cast)
    """
    contract_id = contract.get("id", "unknown")
    info = contract.get("info", {})
    description = info.get("description", f"Auto-generated from {contract_id}")
    model_name = _table_name(contract_id)
    properties = contract.get("schema", {}).get("properties", {})

    columns = []
    for col, clause in properties.items():
        col_entry = {"name": col}
        if "description" in clause:
            col_entry["description"] = clause["description"]

        tests = []

        if clause.get("required"):
            tests.append("not_null")

        if clause.get("unique"):
            tests.append("unique")

        if "enum" in clause:
            tests.append({
                "accepted_values": {
                    "values": clause["enum"],
                }
            })

        if "minimum" in clause or "maximum" in clause:
            between = {}
            if "minimum" in clause:
                between["min_value"] = clause["minimum"]
            if "maximum" in clause:
                between["max_value"] = clause["maximum"]
            tests.append({
                "dbt_expectations.expect_column_values_to_be_between": between
            })

        if clause.get("format") == "uuid":
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                }
            })

        if clause.get("format") == "date-time":
            tests.append({
                "dbt_utils.expression_is_true": {
                    "expression": f"cast({col} as timestamp) is not null"
                }
            })

        if tests:
            col_entry["tests"] = tests

        columns.append(col_entry)

    dbt_schema = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": description,
                "columns": columns,
            }
        ],
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


# ── CLI entry point ──────────────────────────────────────────────────────────

def generate(source: str, contract_id: str, lineage: str | None,
             output_dir: str, annotate: bool = False,
             registry: str | None = None) -> Path:
    """
    Full pipeline: load → flatten → profile → contract → write YAML + dbt.
    Returns the path to the written contract YAML file.
    """
    print(f"[generator] Loading {source}...")
    records = load_jsonl(source)
    print(f"[generator] Loaded {len(records)} records.")

    df = flatten_for_profile(records)
    print(f"[generator] Flattened to DataFrame: {df.shape[0]} rows x {df.shape[1]} cols")

    # ── Step 1: ydata-profiling structural profile ──
    if YDATA_AVAILABLE:
        print("[generator] Running ydata-profiling structural scan...")
        ydata_profiles = _ydata_structural_profile(df)
        print(f"[generator] ydata-profiling complete. Patterns found: {len(ydata_profiles)}")
    else:
        ydata_profiles = {}
        print("[generator] ydata-profiling not available — skipping dominant pattern extraction.")

    column_profiles = profile_all_columns(df)

    # Merge ydata dominant patterns into column profiles
    for col, yp in ydata_profiles.items():
        if col in column_profiles:
            column_profiles[col].update(yp)

    # ── Step 2: confidence sentinel checks ──
    for col, profile in column_profiles.items():
        if "confidence" in col:
            if profile["dtype"] == "object":
                print(
                    f"[generator] WARNING: '{col}' has dtype=object "
                    "(expected float64). Mixed types detected — document in DOMAIN_NOTES.md."
                )
            if "stats" in profile:
                m = profile["stats"]["mean"]
                if m > 0.99:
                    print(
                        f"[generator] WARNING: '{col}' mean={m:.4f} > 0.99 — "
                        "almost certainly clamped at 1.0. Verify producer is not saturating scores."
                    )
                elif m < 0.01:
                    print(
                        f"[generator] WARNING: '{col}' mean={m:.4f} < 0.01 — "
                        "almost certainly broken or zero-filled. Verify extractor is producing real scores."
                    )

    contract = build_contract(records, df, column_profiles, contract_id, source)

    # ── Step 3: lineage injection ──
    contract = inject_lineage(contract, lineage, contract_id, registry_path=registry)

    # ── Step 4: LLM annotation (runs when ANTHROPIC_API_KEY is set) ──
    if annotate or os.environ.get("ANTHROPIC_API_KEY"):
        try:
            contract = _annotate_with_llm(contract, column_profiles, source)
        except Exception as e:
            print(f"[generator] WARNING: LLM annotation failed ({e}). Continuing without annotations.")

    # ── Write Bitol YAML ──
    out_path = Path(output_dir) / f"{contract_id}.yaml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"[generator] Contract written to {out_path}")

    # ── Step 5: dbt schema.yml ──
    dbt_path = Path(output_dir) / f"{contract_id}_dbt.yml"
    generate_dbt_schema(contract, dbt_path)
    print(f"[generator] dbt schema written to {dbt_path}")

    # ── Step 6: Schema snapshot (for SchemaEvolutionAnalyzer) ──
    try:
        from contracts.schema_analyzer import write_schema_snapshot
    except ImportError:
        from schema_analyzer import write_schema_snapshot  # type: ignore[no-redef]
    snap_path = write_schema_snapshot(contract)
    print(f"[generator] Schema snapshot written to {snap_path}")

    return out_path


def _annotate_with_llm(contract: dict, column_profiles: dict, source_path: str) -> dict:
    """
    LLM annotation pass. Calls Claude to produce for each ambiguous column:
      (a) plain-English description
      (b) validation expression
      (c) cross-column relationship note

    Supports two key formats detected automatically from ANTHROPIC_API_KEY:
      sk-ant-*   — native Anthropic API (anthropic SDK)
      sk-or-v1-* — OpenRouter API (openai SDK with OpenRouter base URL)

    Annotation is always attempted when ANTHROPIC_API_KEY is set.
    Results are appended as llm_annotations block in the contract.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("[generator] ANTHROPIC_API_KEY not set — skipping LLM annotation.")
        return contract

    properties = contract.get("schema", {}).get("properties", {})
    annotations = {}

    # Only annotate columns that lack a description and whose name is ambiguous
    ambiguous = [
        col for col, clause in properties.items()
        if "description" not in clause
        and not col.endswith("_id")
        and not col.endswith("_at")
        and "confidence" not in col
    ]

    if not ambiguous:
        return contract

    table_name = contract.get("id", "unknown")
    print(f"[generator] LLM annotating {len(ambiguous)} ambiguous columns...")

    # Detect key type and build a unified call function
    is_openrouter = api_key.startswith("sk-or-v1-")

    def _call_claude(prompt: str) -> str:
        """Call Claude via native Anthropic SDK or OpenRouter (openai-compat)."""
        if is_openrouter:
            import openai
            client = openai.OpenAI(
                api_key=api_key,
                base_url="https://openrouter.ai/api/v1",
            )
            response = client.chat.completions.create(
                model="anthropic/claude-3-5-haiku",
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.choices[0].message.content or ""
        else:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text

    for col in ambiguous[:5]:  # cap at 5 to control cost
        profile = column_profiles.get(col, {})
        sample = profile.get("sample_values", [])
        adjacent = list(properties.keys())[:5]

        prompt = (
            f"You are a data engineer reviewing a data contract for table '{table_name}'.\n"
            f"Column name: {col}\n"
            f"Sample values: {sample}\n"
            f"Adjacent columns: {adjacent}\n\n"
            "Respond with ONLY a JSON object (no markdown, no explanation) with exactly three fields:\n"
            "  description: a plain-English one-sentence description of what this column holds\n"
            "  validation_expression: a Python boolean expression that validates a single value, "
            "e.g. 'isinstance(v, str) and len(v) > 0'\n"
            "  cross_column_note: any relationship with adjacent columns, or null if none\n"
        )

        try:
            text = _call_claude(prompt)
            # Strip markdown code fences if present
            text = text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            annotation = json.loads(text.strip())
            annotations[col] = annotation
            print(f"[generator]   annotated: {col}")
        except Exception as e:
            print(f"[generator]   WARNING: annotation failed for '{col}': {e}")

    if annotations:
        contract["llm_annotations"] = annotations
        print(f"[generator] LLM annotations added for {len(annotations)} column(s).")

    return contract


def main():
    parser = argparse.ArgumentParser(description="Generate a Bitol v3.0.0 data contract from a JSONL source.")
    parser.add_argument("--source", required=True, help="Path to source JSONL file")
    parser.add_argument("--contract-id", required=True, help="Unique contract identifier (use underscores: week3_extractions)")
    parser.add_argument("--lineage", default=None, help="Path to lineage snapshots JSONL")
    parser.add_argument("--registry", default=None,
                        help="Path to contract_registry/subscriptions.yaml (primary source for lineage.downstream)")
    parser.add_argument("--output", default="generated_contracts/", help="Output directory")
    parser.add_argument("--annotate", action="store_true", help="Enable LLM annotation for ambiguous columns (requires ANTHROPIC_API_KEY)")
    args = parser.parse_args()

    generate(
        source=args.source,
        contract_id=args.contract_id,
        lineage=args.lineage,
        output_dir=args.output,
        annotate=args.annotate,
        registry=args.registry,
    )


if __name__ == "__main__":
    main()
