"""
SchemaEvolutionAnalyzer  -  Phase 3
Diffs consecutive schema snapshots for a contract, classifies every change
using the full taxonomy, and generates migration impact reports for breaking
changes.

Backward-compatible exports preserved: write_snapshot, diff_snapshots, print_diff_report.

Phase 3 usage:
    python contracts/schema_analyzer.py \
        --contract-id week3_extractions \
        --since "7 days ago" \
        --output validation_reports/schema_evolution_week3.json \
        --registry contract_registry/subscriptions.yaml

Legacy usage (still supported):
    python contracts/schema_analyzer.py snapshot --contract generated_contracts/week3_extractions.yaml
    python contracts/schema_analyzer.py diff --old <old.json> --new <new.json>
"""

import argparse
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

try:
    from contracts.attributor import load_registry
except ImportError:
    from attributor import load_registry  # type: ignore[no-redef]


# ── Change type constants ─────────────────────────────────────────────────────

ADD_NULLABLE            = "ADD_NULLABLE"             # compatible
ADD_REQUIRED            = "ADD_REQUIRED"             # breaking  -  new field is required
REMOVE_FIELD            = "REMOVE_FIELD"             # breaking
RENAME                  = "RENAME"                   # breaking (heuristic only)
WIDEN_TYPE              = "WIDEN_TYPE"               # compatible (integer -> number)
NARROW_TYPE             = "NARROW_TYPE"              # breaking (number -> integer, scale change)
RANGE_NARROW            = "RANGE_NARROW"             # breaking (min raised or max lowered)
RANGE_WIDEN             = "RANGE_WIDEN"              # compatible
CHANGE_ENUM_ADD         = "CHANGE_ENUM_ADD"          # compatible
CHANGE_ENUM_REMOVE      = "CHANGE_ENUM_REMOVE"       # breaking
ADD_REQUIRED_CONSTRAINT = "ADD_REQUIRED_CONSTRAINT"  # breaking (optional -> required on existing field)

_BREAKING_TYPES = {
    ADD_REQUIRED,
    REMOVE_FIELD,
    RENAME,
    NARROW_TYPE,
    RANGE_NARROW,
    CHANGE_ENUM_REMOVE,
    ADD_REQUIRED_CONSTRAINT,
}

_SEVERITY_RANK = {"CRITICAL": 3, "HIGH": 2, "LOW": 1}


# ── Snapshot management ───────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def write_schema_snapshot(
    contract: dict,
    snapshots_dir: str | Path = "schema_snapshots",
) -> Path:
    """
    Write a timestamped schema snapshot from an in-memory contract dict.
    Called from generator.generate() immediately after the Bitol YAML is written.

    Path: schema_snapshots/{contract_id}/{YYYYmmdd_HHMMSS}_{uuid8}.json
    The uuid8 suffix prevents filename collisions when two runs finish within
    the same second.
    """
    contract_id = contract.get("id", "unknown")
    properties = contract.get("schema", {}).get("properties", {})
    schema_version = contract.get("info", {}).get("version", "1.0.0")

    tag = _now_tag()
    uid = str(uuid.uuid4())[:8]
    out_dir = Path(snapshots_dir) / contract_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{tag}_{uid}.json"

    snapshot = {
        "contract_id": contract_id,
        "snapshot_id": str(uuid.uuid4()),
        "captured_at": _now(),
        "schema_version": schema_version,
        "source_path": contract.get("source", {}).get("path", ""),
        "record_count": contract.get("source", {}).get("record_count", 0),
        "sha256": contract.get("source", {}).get("sha256", ""),
        "properties": properties,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    print(f"[schema_analyzer] Snapshot written: {out_path}")
    return out_path


def write_snapshot(contract_path: str,
                   snapshots_dir: str = "schema_snapshots") -> Path:
    """Backward-compat shim: reads a contract YAML and calls write_schema_snapshot."""
    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    return write_schema_snapshot(contract, snapshots_dir)


def load_snapshot(path: str) -> dict:
    """Load a single snapshot from a JSON file."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def latest_two_snapshots(
    contract_id: str, snapshots_dir: str = "schema_snapshots"
) -> tuple[Path | None, Path | None]:
    """Return (older, newer) snapshot paths. Returns (None, last) if fewer than 2 exist."""
    snap_dir = Path(snapshots_dir) / contract_id
    if not snap_dir.exists():
        return None, None
    snapshots = sorted(snap_dir.glob("*.json"))
    if len(snapshots) < 2:
        return None, snapshots[-1] if snapshots else None
    return snapshots[-2], snapshots[-1]


def _parse_since(since: str) -> datetime:
    """
    Parse 'N days ago' or an ISO date string to a UTC-aware datetime.
    Falls back to 7 days ago with a warning if the format is unrecognised.
    """
    m = re.match(r"^(\d+)\s+days?\s+ago$", since.strip().lower())
    if m:
        return datetime.now(timezone.utc) - timedelta(days=int(m.group(1)))
    try:
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    print(f"[schema_analyzer] WARNING: could not parse --since '{since}', defaulting to 7 days ago.")
    return datetime.now(timezone.utc) - timedelta(days=7)


def _snap_timestamp(snap: dict, path: Path) -> datetime:
    """
    Extract captured_at as a UTC datetime. Tolerates both 'captured_at' (Phase 3)
    and 'snapshot_at' (Phase 2 legacy). Falls back to parsing the filename stem.
    """
    ts_str = snap.get("captured_at") or snap.get("snapshot_at", "")
    if ts_str:
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    # Filename fallback: YYYYmmdd_HHMMSS or YYYYmmdd_HHMMSS_uid8
    parts = path.stem.split("_")
    if len(parts) >= 2:
        try:
            return datetime.strptime(
                f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def load_snapshots(
    contract_id: str,
    snapshots_dir: str | Path = "schema_snapshots",
    since: str = "7 days ago",
) -> list[dict]:
    """
    Load all snapshots for contract_id captured at or after `since`.
    Returns the list sorted by captured_at ascending.
    """
    since_dt = _parse_since(since)
    snap_dir = Path(snapshots_dir) / contract_id

    if not snap_dir.exists():
        return []

    snaps = []
    for fp in snap_dir.glob("*.json"):
        try:
            with open(fp, encoding="utf-8") as f:
                snap = json.load(f)
            if not snap:
                continue
            ts = _snap_timestamp(snap, fp)
            if ts >= since_dt:
                snap["_ts"] = ts
                snap["_loaded_from"] = str(fp)
                snaps.append(snap)
        except Exception as exc:
            print(f"[schema_analyzer] WARNING: skipping bad snapshot {fp}: {exc}")

    snaps.sort(key=lambda s: (s["_ts"], s.get("_loaded_from", "")))
    return snaps


# ── Change classification ─────────────────────────────────────────────────────

def _classify_field_change(
    field: str,
    old_clause: dict,
    new_clause: dict,
) -> list[dict]:
    """
    Compare old and new property clauses for a single field.
    Returns a LIST of change records  -  a field can accumulate multiple independent
    changes (e.g. type narrowed AND enum values removed simultaneously).

    Each record shape:
      {field, change_type, classification, severity, details, migration_note}
    """
    changes: list[dict] = []

    # ── Required constraint ───────────────────────────────────────────────────
    old_req = old_clause.get("required", False)
    new_req = new_clause.get("required", False)
    if not old_req and new_req:
        changes.append({
            "field": field,
            "change_type": ADD_REQUIRED_CONSTRAINT,
            "classification": "breaking",
            "severity": "CRITICAL",
            "details": {"old": {"required": False}, "new": {"required": True}},
            "migration_note": (
                f"Field '{field}' changed from optional to required. "
                "All producers must supply it; existing null records will fail."
            ),
        })
    elif old_req and not new_req:
        changes.append({
            "field": field,
            "change_type": ADD_NULLABLE,
            "classification": "compatible",
            "severity": "LOW",
            "details": {"old": {"required": True}, "new": {"required": False}},
            "migration_note": f"Field '{field}' relaxed from required to optional  -  backward-compatible.",
        })

    # ── Type change ───────────────────────────────────────────────────────────
    old_type = old_clause.get("type")
    new_type = new_clause.get("type")
    if old_type != new_type and old_type and new_type:
        is_widen = (old_type == "integer" and new_type == "number")
        changes.append({
            "field": field,
            "change_type": WIDEN_TYPE if is_widen else NARROW_TYPE,
            "classification": "compatible" if is_widen else "breaking",
            "severity": "LOW" if is_widen else "CRITICAL",
            "details": {"old": {"type": old_type}, "new": {"type": new_type}},
            "migration_note": (
                f"Type widened '{old_type}' -> '{new_type}'  -  backward-compatible."
                if is_widen else
                f"Type change '{old_type}' -> '{new_type}'  -  CRITICAL breaking change. "
                "Consumers expecting the old type will fail or silently misparse data."
            ),
        })

    # ── Range changes ─────────────────────────────────────────────────────────
    old_min = old_clause.get("minimum")
    old_max = old_clause.get("maximum")
    new_min = new_clause.get("minimum")
    new_max = new_clause.get("maximum")

    # Confidence scale change: 0.0-1.0 -> 0-N (N >= 50) is CRITICAL regardless of
    # the generic range logic  -  absorb it into a single NARROW_TYPE record with a
    # domain-specific migration note.
    is_confidence = "confidence" in field.lower()
    confidence_scale_change = (
        is_confidence
        and old_max is not None and float(old_max) <= 1.0
        and new_max is not None and float(new_max) >= 50
    )
    # Also detect inverse (0-100 -> 0-1)
    confidence_scale_reverse = (
        is_confidence
        and old_max is not None and float(old_max) >= 50
        and new_max is not None and float(new_max) <= 1.0
    )

    if confidence_scale_change or confidence_scale_reverse:
        from_range = f"0.0-{old_max}" if confidence_scale_change else f"0-{old_max}"
        to_range = f"0-{new_max}" if confidence_scale_change else f"0.0-{new_max}"
        changes.append({
            "field": field,
            "change_type": NARROW_TYPE,
            "classification": "breaking",
            "severity": "CRITICAL",
            "details": {
                "detected_scale_change": True,
                "old": {"minimum": old_min, "maximum": old_max},
                "new": {"minimum": new_min, "maximum": new_max},
            },
            "migration_note": (
                f"Confidence scale changed {from_range} -> {to_range}. "
                "CRITICAL: all downstream consumers using confidence as a probability "
                "(drift detection, edge-weight ranking) will be silently corrupted. "
                "Statistical baselines must be deleted and re-established post-migration."
            ),
        })
    else:
        # Minimum changes
        if (old_min is None and new_min is not None) or (
            old_min is not None and new_min is not None
            and float(new_min) > float(old_min)
        ):
            changes.append({
                "field": field,
                "change_type": RANGE_NARROW,
                "classification": "breaking",
                "severity": "HIGH",
                "details": {"old": {"minimum": old_min}, "new": {"minimum": new_min}},
                "migration_note": (
                    f"Minimum raised {old_min} -> {new_min}. "
                    f"Values in [{old_min}, {new_min}) are now out of range."
                ),
            })
        elif (old_min is not None and new_min is None) or (
            old_min is not None and new_min is not None
            and float(new_min) < float(old_min)
        ):
            changes.append({
                "field": field,
                "change_type": RANGE_WIDEN,
                "classification": "compatible",
                "severity": "LOW",
                "details": {"old": {"minimum": old_min}, "new": {"minimum": new_min}},
                "migration_note": f"Minimum lowered {old_min} -> {new_min}  -  backward-compatible.",
            })

        # Maximum changes
        if (old_max is None and new_max is not None) or (
            old_max is not None and new_max is not None
            and float(new_max) < float(old_max)
        ):
            changes.append({
                "field": field,
                "change_type": RANGE_NARROW,
                "classification": "breaking",
                "severity": "HIGH",
                "details": {"old": {"maximum": old_max}, "new": {"maximum": new_max}},
                "migration_note": (
                    f"Maximum decreased {old_max} -> {new_max}. "
                    f"Values in ({new_max}, {old_max}] are now out of range."
                ),
            })
        elif (old_max is not None and new_max is None) or (
            old_max is not None and new_max is not None
            and float(new_max) > float(old_max)
        ):
            changes.append({
                "field": field,
                "change_type": RANGE_WIDEN,
                "classification": "compatible",
                "severity": "LOW",
                "details": {"old": {"maximum": old_max}, "new": {"maximum": new_max}},
                "migration_note": f"Maximum raised {old_max} -> {new_max}  -  backward-compatible.",
            })

    # ── Enum changes ──────────────────────────────────────────────────────────
    old_enum = set(old_clause.get("enum") or [])
    new_enum = set(new_clause.get("enum") or [])
    if old_enum != new_enum:
        removed_vals = sorted(old_enum - new_enum)
        added_vals = sorted(new_enum - old_enum)
        if removed_vals:
            changes.append({
                "field": field,
                "change_type": CHANGE_ENUM_REMOVE,
                "classification": "breaking",
                "severity": "HIGH",
                "details": {
                    "removed": removed_vals,
                    "old": sorted(old_enum),
                    "new": sorted(new_enum),
                },
                "migration_note": (
                    f"Enum values removed: {removed_vals}. "
                    "Existing data with those values now fails validation. "
                    "Each registry subscriber must be audited for hardcoded references."
                ),
            })
        if added_vals:
            changes.append({
                "field": field,
                "change_type": CHANGE_ENUM_ADD,
                "classification": "compatible",
                "severity": "LOW",
                "details": {
                    "added": added_vals,
                    "old": sorted(old_enum),
                    "new": sorted(new_enum),
                },
                "migration_note": (
                    f"Enum values added: {added_vals}. "
                    "Notify all subscribers  -  backward-compatible."
                ),
            })

    # ── Format change ─────────────────────────────────────────────────────────
    old_fmt = old_clause.get("format")
    new_fmt = new_clause.get("format")
    if old_fmt != new_fmt:
        changes.append({
            "field": field,
            "change_type": NARROW_TYPE,
            "classification": "breaking",
            "severity": "HIGH",
            "details": {"old": {"format": old_fmt}, "new": {"format": new_fmt}},
            "migration_note": (
                f"Format changed '{old_fmt}' -> '{new_fmt}'. "
                "Pattern validation will reject data valid under the old format."
            ),
        })

    return changes


def _detect_renames(
    removed: list[str],
    added: list[str],
    old_props: dict,
    new_props: dict,
) -> list[dict]:
    """
    Heuristic: a removed field and an added field with the same type are a possible
    rename. confidence=0.8 when 2+ constraints also match, 0.5 otherwise.
    """
    renames = []
    for r in removed:
        for a in added:
            old_c = old_props[r]
            new_c = new_props[a]
            if old_c.get("type") != new_c.get("type"):
                continue
            matching = sum(
                1 for k in ("minimum", "maximum", "format", "required")
                if old_c.get(k) == new_c.get(k)
            )
            renames.append({
                "old_field": r,
                "new_field": a,
                "confidence": 0.8 if matching >= 2 else 0.5,
                "reason": (
                    f"Same type ({old_c.get('type')}) "
                    f"and {matching} matching constraints suggest rename. "
                    "Minimum 2-sprint deprecation with alias required before removing old name."
                ),
            })
    return renames


def diff_schemas(
    old_snapshot: dict,
    new_snapshot: dict,
) -> tuple[list[dict], list[str], list[str], list[dict]]:
    """
    Diff two snapshot dicts.
    Returns (changes, added_fields, removed_fields, possible_renames).
    `changes` is the flat list of all change records across every field.
    """
    old_props = old_snapshot.get("properties", {})
    new_props = new_snapshot.get("properties", {})

    added_fields = sorted(set(new_props) - set(old_props))
    removed_fields = sorted(set(old_props) - set(new_props))
    modified_fields = sorted(set(old_props) & set(new_props))

    changes: list[dict] = []

    for field in added_fields:
        clause = new_props[field]
        if clause.get("required", False):
            changes.append({
                "field": field,
                "change_type": ADD_REQUIRED,
                "classification": "breaking",
                "severity": "CRITICAL",
                "details": {"old": None, "new": clause},
                "migration_note": (
                    f"New required field '{field}' added. "
                    "Producers must supply it; existing records without it fail validation."
                ),
            })
        else:
            changes.append({
                "field": field,
                "change_type": ADD_NULLABLE,
                "classification": "compatible",
                "severity": "LOW",
                "details": {"old": None, "new": clause},
                "migration_note": (
                    f"New optional field '{field}' added  -  backward-compatible."
                ),
            })

    for field in removed_fields:
        changes.append({
            "field": field,
            "change_type": REMOVE_FIELD,
            "classification": "breaking",
            "severity": "CRITICAL",
            "details": {"old": old_props[field], "new": None},
            "migration_note": (
                f"Field '{field}' removed. "
                "Two-sprint minimum deprecation required. "
                "Each registry subscriber must acknowledge the removal."
            ),
        })

    for field in modified_fields:
        changes.extend(_classify_field_change(field, old_props[field], new_props[field]))

    possible_renames = _detect_renames(removed_fields, added_fields, old_props, new_props)
    return changes, added_fields, removed_fields, possible_renames


# ── Registry integration ──────────────────────────────────────────────────────

def _field_matches(failing_field: str, breaking_fields_spec: list) -> bool:
    """True if failing_field equals or is a sub-path of any declared breaking field."""
    for bf in breaking_fields_spec:
        f = bf["field"] if isinstance(bf, dict) else str(bf)
        if (failing_field == f
                or failing_field.startswith(f + ".")
                or f.startswith(failing_field + ".")):
            return True
    return False


def _registry_blast_radius(
    contract_id: str,
    breaking_changes: list[dict],
    subscriptions: list[dict],
) -> dict:
    """
    Compute blast radius from registry subscriptions.
    A subscriber is included if any of its breaking_fields matches any changed field.
    Each subscriber appears at most once even if multiple fields match.
    """
    affected: dict[str, dict] = {}

    for change in breaking_changes:
        field = change["field"]
        for sub in subscriptions:
            if sub.get("contract_id") != contract_id:
                continue
            if not _field_matches(field, sub.get("breaking_fields", [])):
                continue
            sid = sub.get("subscriber_id", "unknown")
            if sid not in affected:
                affected[sid] = {
                    "subscriber_id": sid,
                    "subscriber_team": sub.get("subscriber_team"),
                    "contact": sub.get("contact"),
                    "validation_mode": sub.get("validation_mode"),
                    "affected_fields": [],
                }
            if field not in affected[sid]["affected_fields"]:
                affected[sid]["affected_fields"].append(field)

    return {
        "registry_subscribers": list(affected.values()),
        "total_subscribers_affected": len(affected),
    }


def _per_consumer_failure_modes(
    contract_id: str,
    breaking_changes: list[dict],
    subscriptions: list[dict],
) -> list[dict]:
    """
    For each affected subscriber, describe exactly how each breaking change will
    fail them, using the declared reason from subscriptions.yaml.
    """
    result = []

    for sub in subscriptions:
        if sub.get("contract_id") != contract_id:
            continue
        breaking_fields_spec = sub.get("breaking_fields", [])
        failures = []

        for change in breaking_changes:
            field = change["field"]
            for bf in breaking_fields_spec:
                bf_field = bf["field"] if isinstance(bf, dict) else str(bf)
                if not (field == bf_field
                        or field.startswith(bf_field + ".")
                        or bf_field.startswith(field + ".")):
                    continue
                declared_reason = bf.get("reason", "") if isinstance(bf, dict) else ""
                failures.append({
                    "field": field,
                    "change_type": change["change_type"],
                    "declared_reason": declared_reason,
                    "severity": change.get("severity", "HIGH"),
                })
                break  # matched  -  don't duplicate per breaking_field entry

        if failures:
            highest = max(
                failures,
                key=lambda f: _SEVERITY_RANK.get(f["severity"], 0)
            )["severity"]
            result.append({
                "subscriber_id": sub.get("subscriber_id"),
                "subscriber_team": sub.get("subscriber_team"),
                "contact": sub.get("contact"),
                "validation_mode": sub.get("validation_mode"),
                "failures": failures,
                "highest_severity": highest,
            })

    return result


# ── Migration planning ────────────────────────────────────────────────────────

def _migration_checklist(
    breaking_changes: list[dict],
    consumers: list[dict],
) -> list[str]:
    """
    Generate an ordered migration checklist based on the types of breaking changes
    and which consumers are affected.
    """
    checklist: list[str] = []
    step = 1
    change_types = {c["change_type"] for c in breaking_changes}
    subscriber_ids = [c["subscriber_id"] for c in consumers]

    # 1. Always notify first
    if subscriber_ids:
        checklist.append(
            f"{step}. Notify all registered subscribers of breaking changes: "
            f"{', '.join(subscriber_ids)}."
        )
        step += 1

    # 2. REMOVE_FIELD  -  consumers must stop reading before field is dropped
    if REMOVE_FIELD in change_types:
        fields = [c["field"] for c in breaking_changes if c["change_type"] == REMOVE_FIELD]
        checklist.append(
            f"{step}. Fields scheduled for removal: {fields}. "
            "Minimum two-sprint deprecation period. "
            "Each subscriber must acknowledge the removal before the field is dropped."
        )
        step += 1

    # 3. RENAME  -  alias period
    if RENAME in change_types:
        checklist.append(
            f"{step}. Implement deprecation alias: keep the old field name populated "
            "for a minimum of two sprints while consumers migrate to the new name. "
            "Both old and new names must coexist during the transition window."
        )
        step += 1

    # 4. NARROW_TYPE / ADD_REQUIRED / ADD_REQUIRED_CONSTRAINT  -  coordinate producers
    if NARROW_TYPE in change_types or ADD_REQUIRED in change_types or ADD_REQUIRED_CONSTRAINT in change_types:
        scale_changes = [
            c for c in breaking_changes
            if c["change_type"] == NARROW_TYPE
            and c["details"].get("detected_scale_change")
        ]
        if scale_changes:
            checklist.append(
                f"{step}. Write migration script to convert confidence fields from old scale "
                "to new scale. Include a rollback script that restores original values. "
                "Confirm via ValidationRunner --mode ENFORCE before deploying to production."
            )
            step += 1
            checklist.append(
                f"{step}. Delete and re-establish statistical baselines: "
                "remove schema_snapshots/baselines.json and re-run ValidationRunner "
                "against a fresh sample after migration."
            )
            step += 1
        else:
            checklist.append(
                f"{step}. For type/required changes: coordinate with all producers to "
                "supply default values or run a backfill before deploy. "
                "Block deploy until all producers are updated."
            )
            step += 1

    # 5. RANGE_NARROW
    if RANGE_NARROW in change_types:
        checklist.append(
            f"{step}. Write range migration script for narrowed fields. "
            "Audit existing data for out-of-range values before applying the new constraint."
        )
        step += 1

    # 6. CHANGE_ENUM_REMOVE
    if CHANGE_ENUM_REMOVE in change_types:
        checklist.append(
            f"{step}. Enum values removed. "
            "Audit all consumers for hardcoded references to removed values. "
            "Run a full data scan to confirm no existing records carry removed values."
        )
        step += 1

    # 7. Update consumer code (per consumer)
    for consumer in consumers:
        sid = consumer["subscriber_id"]
        fields = [f["field"] for f in consumer["failures"]]
        checklist.append(
            f"{step}. Update {sid}: adapt to changes in fields {fields}. "
            f"Contact: {consumer.get('contact', 'unknown')}."
        )
        step += 1

    # 8. Coordinate deploy
    checklist.append(
        f"{step}. Coordinate simultaneous deploy of producers and consumers. "
        "Do not deploy producers before consumers have been updated."
    )
    step += 1

    # 9. Post-deploy validation
    checklist.append(
        f"{step}. Re-run ContractGenerator and ValidationRunner --mode ENFORCE "
        "post-deploy to confirm the new contract snapshot matches the expected schema."
    )
    step += 1

    # 10. Registry update
    checklist.append(
        f"{step}. Update contract_registry/subscriptions.yaml if any subscriber "
        "breaking_fields declarations have changed as a result of this migration."
    )

    return checklist


def _rollback_plan(
    breaking_changes: list[dict],
    contract_id: str,
) -> list[str]:
    """Generate an ordered rollback plan anchored to the contract_id and change types."""
    plan: list[str] = []
    step = 1
    change_types = {c["change_type"] for c in breaking_changes}

    plan.append(
        f"{step}. Stop all consumers of '{contract_id}' before reverting producers "
        "to prevent partial-migration data from reaching downstream systems."
    )
    step += 1

    if NARROW_TYPE in change_types or RANGE_NARROW in change_types:
        plan.append(
            f"{step}. Execute rollback script to restore original type/range in existing data. "
            f"Verify with: python contracts/runner.py "
            f"--contract generated_contracts/{contract_id}.yaml "
            f"--data <data_path> --mode ENFORCE"
        )
        step += 1

    if REMOVE_FIELD in change_types:
        plan.append(
            f"{step}. Restore removed fields by reverting to the previous contract snapshot: "
            f"schema_snapshots/{contract_id}/<previous_timestamp>.json"
        )
        step += 1

    if ADD_REQUIRED in change_types or ADD_REQUIRED_CONSTRAINT in change_types:
        fields = [
            c["field"] for c in breaking_changes
            if c["change_type"] in (ADD_REQUIRED, ADD_REQUIRED_CONSTRAINT)
        ]
        plan.append(
            f"{step}. Revert required constraints on {fields}  -  make them optional "
            "or remove the new fields entirely from the contract."
        )
        step += 1

    if CHANGE_ENUM_REMOVE in change_types:
        plan.append(
            f"{step}. Restore removed enum values in the contract schema. "
            "Re-run ContractGenerator from the previous data snapshot to regenerate the enum list."
        )
        step += 1

    plan.append(
        f"{step}. Re-run ContractGenerator to regenerate contract YAML from the original data."
    )
    step += 1

    plan.append(
        f"{step}. Re-run ValidationRunner in ENFORCE mode to confirm all checks pass: "
        f"python contracts/runner.py --contract generated_contracts/{contract_id}.yaml "
        f"--data <data_path> --mode ENFORCE"
    )
    step += 1

    plan.append(
        f"{step}. Notify all registered subscribers that the rollback is complete "
        "and the original schema has been restored."
    )

    return plan


# ── Human-readable diff ───────────────────────────────────────────────────────

def _human_readable_diff(changes: list[dict]) -> list[str]:
    """
    Render each change record as a compact, human-readable text block.
    This is the "exact diff" section of the migration impact report  -  the
    document you hand to the team lead.

    Format per change:
      [BREAKING|compatible] field_name  (CHANGE_TYPE)
        was:  <old values or "(absent)">
        now:  <new values or "(removed)">
        note: <migration_note>
    """
    lines = []
    for c in changes:
        tag = "BREAKING " if c.get("classification") == "breaking" else "compatible"
        severity = c.get("severity", "")
        header = f"[{tag}| {severity}] {c['field']}  ({c['change_type']})"
        lines.append(header)

        details = c.get("details", {})
        old_val = details.get("old")
        new_val = details.get("new")

        if old_val is None and new_val is not None:
            lines.append(f"    was:  (absent)")
            lines.append(f"    now:  {json.dumps(new_val, separators=(',', ':'))}")
        elif old_val is not None and new_val is None:
            lines.append(f"    was:  {json.dumps(old_val, separators=(',', ':'))}")
            lines.append(f"    now:  (removed)")
        else:
            # Show only the attributes that actually changed
            old_show = {k: v for k, v in (old_val or {}).items()
                        if k not in ("detected_scale_change",)}
            new_show = {k: v for k, v in (new_val or {}).items()
                        if k not in ("detected_scale_change",)}
            lines.append(f"    was:  {json.dumps(old_show, separators=(',', ':'))}")
            lines.append(f"    now:  {json.dumps(new_show, separators=(',', ':'))}")

        lines.append(f"    note: {c.get('migration_note', '')}")
        lines.append("")

    return lines


# ── Lineage graph enrichment (optional) ──────────────────────────────────────

def _lineage_blast_radius(
    lineage_path: str,
    data_source_path: str,
) -> dict | None:
    """
    BFS the Week 4 lineage graph to find transitive downstream contamination.
    Returns {affected_nodes, contamination_depth, codebase_root} or None on failure.
    """
    try:
        from contracts.attributor import (
            load_latest_snapshot, build_adjacency,
            find_source_node, bfs_downstream, _max_contamination_depth,
        )
    except ImportError:
        try:
            from attributor import (  # type: ignore[no-redef]
                load_latest_snapshot, build_adjacency,
                find_source_node, bfs_downstream, _max_contamination_depth,
            )
        except ImportError:
            return None

    try:
        snapshot = load_latest_snapshot(lineage_path)
        forward, _ = build_adjacency(snapshot)
        source_node = find_source_node(snapshot, data_source_path)
        if not source_node:
            return None
        affected = bfs_downstream(source_node, forward)
        depth = _max_contamination_depth(source_node, forward)
        return {
            "affected_nodes": affected,
            "contamination_depth": depth,
            "codebase_root": snapshot.get("codebase_root"),
        }
    except Exception as exc:
        print(f"[schema_analyzer] WARNING: lineage enrichment failed: {exc}")
        return None


# ── Migration impact report ───────────────────────────────────────────────────

def generate_migration_impact(
    changes: list[dict],
    possible_renames: list[dict],
    old_snapshot: dict,
    new_snapshot: dict,
    subscriptions: list[dict],
    contract_id: str,
    output_dir: str | Path,
    lineage_path: str | None = None,
) -> dict:
    """
    Assemble the full migration impact report for breaking changes and write it
    to output_dir/migration_impact_{contract_id}_{timestamp}.json.

    Includes:
      - exact diff (human-readable lines)
      - compatibility verdict
      - full blast radius: registry subscribers (primary) + lineage BFS (enrichment)
      - per-consumer failure mode analysis with declared reasons
      - ordered migration checklist
      - rollback plan

    Returns the report dict.
    """
    breaking = [c for c in changes if c.get("classification") == "breaking"]
    compatible = [c for c in changes if c.get("classification") == "compatible"]

    # Primary blast radius: registry
    blast_radius = _registry_blast_radius(contract_id, breaking, subscriptions)

    # Enrichment: lineage graph BFS (if a lineage snapshot is available)
    if lineage_path:
        data_path = old_snapshot.get("source_path", "")
        lineage_info = _lineage_blast_radius(lineage_path, data_path)
        if lineage_info:
            blast_radius["lineage_affected_nodes"] = lineage_info["affected_nodes"]
            blast_radius["lineage_contamination_depth"] = lineage_info["contamination_depth"]
            blast_radius["lineage_codebase_root"] = lineage_info.get("codebase_root")

    consumers = _per_consumer_failure_modes(contract_id, breaking, subscriptions)
    checklist = _migration_checklist(breaking, consumers)
    rollback = _rollback_plan(breaking, contract_id)
    diff_human = _human_readable_diff(changes)

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": _now(),
        "from_snapshot": {
            "snapshot_id": old_snapshot.get("snapshot_id"),
            "captured_at": old_snapshot.get("captured_at") or old_snapshot.get("snapshot_at"),
        },
        "to_snapshot": {
            "snapshot_id": new_snapshot.get("snapshot_id"),
            "captured_at": new_snapshot.get("captured_at") or new_snapshot.get("snapshot_at"),
        },
        "compatibility_verdict": "BREAKING",
        "breaking_change_count": len(breaking),
        "compatible_change_count": len(compatible),
        "changes": changes,
        "possible_renames": possible_renames,
        "diff": {
            "added": [c["field"] for c in changes
                      if c["change_type"] in (ADD_NULLABLE, ADD_REQUIRED)],
            "removed": [c["field"] for c in changes if c["change_type"] == REMOVE_FIELD],
            "modified": sorted({
                c["field"] for c in changes
                if c["change_type"] not in (ADD_NULLABLE, ADD_REQUIRED, REMOVE_FIELD)
            }),
            "human_readable": diff_human,
        },
        "blast_radius": blast_radius,
        "per_consumer_failure_modes": consumers,
        "migration_checklist": checklist,
        "rollback_plan": rollback,
    }

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(output_dir) / f"migration_impact_{contract_id}_{timestamp}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"[schema_analyzer] Migration impact report: {out_path}")
    return report


# ── Main analysis pipeline ────────────────────────────────────────────────────

def analyze(
    contract_id: str,
    since: str = "7 days ago",
    output_path: str | None = None,
    snapshots_dir: str | Path = "schema_snapshots",
    registry_path: str | Path = "contract_registry/subscriptions.yaml",
    lineage_path: str | None = None,
) -> dict | None:
    """
    Main Phase 3 pipeline:
      1. Load snapshots in the time window.
      2. Diff the two most recent consecutive snapshots.
      3. If breaking changes exist, generate migration impact report (with
         lineage graph enrichment when --lineage is provided).
      4. Write analysis summary to output_path.

    Returns None if fewer than 2 snapshots are found in the window.
    """
    snapshots = load_snapshots(contract_id, snapshots_dir, since)

    if len(snapshots) < 2:
        print(
            f"[schema_analyzer] Only {len(snapshots)} snapshot(s) found for "
            f"'{contract_id}' since '{since}'. Need at least 2 to diff."
        )
        return None

    old_snapshot = snapshots[-2]
    new_snapshot = snapshots[-1]

    subscriptions = load_registry(str(registry_path))

    changes, added, removed, possible_renames = diff_schemas(old_snapshot, new_snapshot)

    breaking = [c for c in changes if c.get("classification") == "breaking"]
    verdict = "BREAKING" if breaking else ("COMPATIBLE" if changes else "UNCHANGED")

    result: dict = {
        "contract_id": contract_id,
        "analyzed_at": _now(),
        "since": since,
        "snapshots_compared": 2,
        "from_snapshot": {
            "snapshot_id": old_snapshot.get("snapshot_id"),
            "captured_at": old_snapshot.get("captured_at") or old_snapshot.get("snapshot_at"),
            "loaded_from": old_snapshot.get("_loaded_from"),
        },
        "to_snapshot": {
            "snapshot_id": new_snapshot.get("snapshot_id"),
            "captured_at": new_snapshot.get("captured_at") or new_snapshot.get("snapshot_at"),
            "loaded_from": new_snapshot.get("_loaded_from"),
        },
        "compatibility_verdict": verdict,
        "change_count": len(changes),
        "breaking_change_count": len(breaking),
        "changes": changes,
        "possible_renames": possible_renames,
    }

    if breaking:
        output_dir = str(Path(output_path).parent) if output_path else "."
        impact = generate_migration_impact(
            changes, possible_renames, old_snapshot, new_snapshot,
            subscriptions, contract_id, output_dir,
            lineage_path=lineage_path,
        )
        result["migration_impact_path"] = impact.get(
            "_out_path",
            str(Path(output_dir) / f"migration_impact_{contract_id}_latest.json"),
        )

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(f"[schema_analyzer] Analysis written: {output_path}")

    return result


# ── Backward-compat wrappers ──────────────────────────────────────────────────

def diff_snapshots(old_path: str, new_path: str) -> dict:
    """
    Backward-compat: diff two snapshot JSON files by path.
    Returns a report shaped like the Phase 2 output for existing callers.
    """
    old = load_snapshot(old_path)
    new = load_snapshot(new_path)

    changes, added, removed, possible_renames = diff_schemas(old, new)

    breaking = [c for c in changes if c.get("classification") == "breaking"]
    compatible = [c for c in changes if c.get("classification") == "compatible"]
    overall = "BREAKING" if breaking else ("COMPATIBLE" if compatible else "UNCHANGED")

    # Map new change records to the legacy field_changes shape
    field_changes = []
    for c in changes:
        field_changes.append({
            "column": c["field"],
            "change_type": c["change_type"].lower(),
            "classification": "BREAKING" if c["classification"] == "breaking" else "COMPATIBLE",
            "migration_note": c["migration_note"],
            "details": c.get("details", {}),
        })

    return {
        "contract_id": old.get("contract_id", "unknown"),
        "old_version": old.get("schema_version") or old.get("contract_version"),
        "new_version": new.get("schema_version") or new.get("contract_version"),
        "old_snapshot_at": old.get("captured_at") or old.get("snapshot_at"),
        "new_snapshot_at": new.get("captured_at") or new.get("snapshot_at"),
        "overall_classification": overall,
        "total_changes": len(changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(compatible),
        "field_changes": field_changes,
        "possible_renames": possible_renames,
        "migration_required": len(breaking) > 0,
        "migration_notes": [c["migration_note"] for c in breaking],
    }


def print_diff_report(diff: dict) -> None:
    print(f"\n[schema_analyzer] Contract : {diff['contract_id']}")
    print(f"[schema_analyzer] Versions : {diff.get('old_version')} -> {diff.get('new_version')}")
    print(f"[schema_analyzer] Overall  : {diff['overall_classification']}")
    print(
        f"[schema_analyzer] Changes  : {diff['total_changes']} "
        f"(BREAKING={diff['breaking_changes']}, COMPATIBLE={diff['compatible_changes']})"
    )
    for change in diff.get("field_changes", []):
        marker = "BREAKING" if change["classification"] == "BREAKING" else "compatible"
        print(f"  [{marker}] {change['column']}: {change['change_type']}")
        print(f"    Note: {change['migration_note']}")

    renames = diff.get("possible_renames", [])
    if renames:
        print(f"\n[schema_analyzer] Possible renames detected ({len(renames)}):")
        for r in renames:
            print(f"  {r['old_field']} -> {r['new_field']} (confidence={r['confidence']}): {r['reason']}")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Schema snapshot and evolution analysis.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Phase 3 mode (auto-diff + migration impact):\n"
            "  python contracts/schema_analyzer.py \\\n"
            "      --contract-id week3_extractions --since '7 days ago' \\\n"
            "      --output validation_reports/schema_evolution_week3.json\n\n"
            "Legacy subcommands:\n"
            "  snapshot --contract generated_contracts/week3_extractions.yaml\n"
            "  diff --old <old.json> --new <new.json>"
        ),
    )

    # Phase 3 top-level flags (no subcommand)
    parser.add_argument("--contract-id", help="Contract ID to analyze (Phase 3 mode)")
    parser.add_argument("--since", default="7 days ago", help="Time window, e.g. '7 days ago'")
    parser.add_argument("--output", default=None, help="Write analysis JSON to this path")
    parser.add_argument(
        "--snapshots-dir", default="schema_snapshots",
        help="Root directory for schema snapshots"
    )
    parser.add_argument(
        "--registry", default="contract_registry/subscriptions.yaml",
        help="Path to subscriptions.yaml"
    )
    parser.add_argument(
        "--lineage", default=None,
        help="Path to lineage snapshots JSONL (Week 4 output) for BFS contamination depth"
    )

    # Legacy subcommands
    sub = parser.add_subparsers(dest="command")

    snap_p = sub.add_parser("snapshot", help="Write a snapshot from a contract YAML")
    snap_p.add_argument("--contract", required=True)
    snap_p.add_argument("--snapshots-dir", default="schema_snapshots")

    diff_p = sub.add_parser("diff", help="Diff two snapshot JSON files")
    diff_p.add_argument("--old", required=True)
    diff_p.add_argument("--new", required=True)
    diff_p.add_argument("--output", default=None, help="Write diff JSON to this path")

    args = parser.parse_args()

    # ── Legacy subcommands ──
    if args.command == "snapshot":
        write_snapshot(args.contract, args.snapshots_dir)

    elif args.command == "diff":
        diff = diff_snapshots(args.old, args.new)
        print_diff_report(diff)
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(diff, f, indent=2)
            print(f"[schema_analyzer] Diff written to {args.output}")

    # ── Phase 3 mode ──
    elif args.contract_id:
        result = analyze(
            contract_id=args.contract_id,
            since=args.since,
            output_path=args.output,
            snapshots_dir=args.snapshots_dir,
            registry_path=args.registry,
            lineage_path=args.lineage,
        )
        if result:
            verdict = result["compatibility_verdict"]
            n_breaking = result["breaking_change_count"]
            n_total = result["change_count"]
            print(
                f"\n[schema_analyzer] {args.contract_id}: {verdict} "
                f"({n_breaking} breaking, {n_total - n_breaking} compatible)"
            )
            if result.get("possible_renames"):
                print(
                    f"[schema_analyzer] Possible renames detected: "
                    f"{[(r['old_field'], r['new_field']) for r in result['possible_renames']]}"
                )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
