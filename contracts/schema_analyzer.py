"""
SchemaEvolutionAnalyzer — Phase 2
Writes a timestamped snapshot of a contract on every generator run, then diffs
two snapshots and classifies each change as BREAKING or COMPATIBLE.

Usage:
    # Write a snapshot after generation:
    python contracts/schema_analyzer.py snapshot \
        --contract generated_contracts/week3_extractions.yaml

    # Diff two snapshots:
    python contracts/schema_analyzer.py diff \
        --old schema_snapshots/week3_extractions/20250115_140000.json \
        --new schema_snapshots/week3_extractions/20250116_090000.json
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ── Snapshot writing ──────────────────────────────────────────────────────────

def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def write_snapshot(contract_path: str,
                   snapshots_dir: str = "schema_snapshots") -> Path:
    """
    Read a contract YAML and write a timestamped JSON snapshot to
    schema_snapshots/<contract_id>/<timestamp>.json.
    Returns the snapshot path.
    """
    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    contract_id = contract.get("id", Path(contract_path).stem)
    properties = contract.get("schema", {}).get("properties", {})
    version = (
        contract.get("info", {}).get("version")
        or contract.get("version", "1.0.0")
    )

    snapshot = {
        "contract_id": contract_id,
        "contract_version": version,
        "snapshot_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_path": contract.get("source", {}).get("path", ""),
        "record_count": contract.get("source", {}).get("record_count", 0),
        "sha256": contract.get("source", {}).get("sha256", ""),
        "properties": properties,
    }

    out_dir = Path(snapshots_dir) / contract_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{_now_tag()}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    print(f"[schema_analyzer] Snapshot written: {out_path}")
    return out_path


def load_snapshot(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def latest_two_snapshots(contract_id: str,
                         snapshots_dir: str = "schema_snapshots") -> tuple[Path | None, Path | None]:
    """
    Return (older, newer) snapshot paths for a contract_id.
    Returns (None, None) if fewer than 2 snapshots exist.
    """
    snap_dir = Path(snapshots_dir) / contract_id
    if not snap_dir.exists():
        return None, None
    snapshots = sorted(snap_dir.glob("*.json"))
    if len(snapshots) < 2:
        return None, snapshots[-1] if snapshots else None
    return snapshots[-2], snapshots[-1]


# ── Change classification ─────────────────────────────────────────────────────

# Confluent Schema Registry compatibility taxonomy (simplified):
#   BREAKING:   old consumer cannot read new data
#   COMPATIBLE: old consumer can still read new data (may miss new fields)

def _classify_field_change(col: str,
                            old_clause: dict | None,
                            new_clause: dict | None) -> dict:
    """
    Compare old and new clauses for a single field.
    Return a change record: {column, change_type, classification, migration_note, details}.
    """
    # ── Field removed ──
    if old_clause is not None and new_clause is None:
        severity = "BREAKING" if old_clause.get("required") else "COMPATIBLE"
        return {
            "column": col,
            "change_type": "field_removed",
            "classification": severity,
            "migration_note": (
                f"Required field '{col}' removed — all consumers reading this field will crash."
                if severity == "BREAKING"
                else f"Optional field '{col}' removed — consumers ignoring it are unaffected."
            ),
            "details": {"old": old_clause, "new": None},
        }

    # ── Field added ──
    if old_clause is None and new_clause is not None:
        severity = "BREAKING" if new_clause.get("required") else "COMPATIBLE"
        return {
            "column": col,
            "change_type": "field_added",
            "classification": severity,
            "migration_note": (
                f"New required field '{col}' added — existing records missing this field will fail required check."
                if severity == "BREAKING"
                else f"New optional field '{col}' added — backward-compatible."
            ),
            "details": {"old": None, "new": new_clause},
        }

    # ── Both exist — check for mutations ──
    changes = []
    classification = "COMPATIBLE"
    notes = []

    old_type = old_clause.get("type")
    new_type = new_clause.get("type")
    if old_type != new_type:
        changes.append(f"type: {old_type} → {new_type}")
        classification = "BREAKING"
        notes.append(f"Type change '{old_type}'→'{new_type}' — consumers expecting {old_type} will fail or misparse.")

    old_req = old_clause.get("required", False)
    new_req = new_clause.get("required", False)
    if not old_req and new_req:
        changes.append("required: false → true")
        classification = "BREAKING"
        notes.append("Field became required — existing records with nulls will now fail.")
    elif old_req and not new_req:
        changes.append("required: true → false")
        notes.append("Field became optional — backward-compatible.")

    old_min = old_clause.get("minimum")
    new_min = new_clause.get("minimum")
    if old_min != new_min:
        changes.append(f"minimum: {old_min} → {new_min}")
        if new_min is not None and (old_min is None or float(new_min) > float(old_min)):
            classification = "BREAKING"
            notes.append(f"Minimum raised from {old_min} to {new_min} — values in [{old_min}, {new_min}) now fail.")
        else:
            notes.append(f"Minimum lowered from {old_min} to {new_min} — backward-compatible.")

    old_max = old_clause.get("maximum")
    new_max = new_clause.get("maximum")
    if old_max != new_max:
        changes.append(f"maximum: {old_max} → {new_max}")
        if new_max is not None and (old_max is None or float(new_max) < float(old_max)):
            classification = "BREAKING"
            notes.append(f"Maximum lowered from {old_max} to {new_max} — values in ({new_max}, {old_max}] now fail.")
        else:
            notes.append(f"Maximum raised from {old_max} to {new_max} — backward-compatible.")

    old_enum = sorted(old_clause.get("enum") or [])
    new_enum = sorted(new_clause.get("enum") or [])
    if old_enum != new_enum:
        removed = set(old_enum) - set(new_enum)
        added = set(new_enum) - set(old_enum)
        if removed:
            changes.append(f"enum values removed: {sorted(removed)}")
            classification = "BREAKING"
            notes.append(f"Enum values removed {sorted(removed)} — existing data with those values now fails.")
        if added:
            changes.append(f"enum values added: {sorted(added)}")
            notes.append(f"Enum values added {sorted(added)} — backward-compatible.")

    old_fmt = old_clause.get("format")
    new_fmt = new_clause.get("format")
    if old_fmt != new_fmt:
        changes.append(f"format: {old_fmt} → {new_fmt}")
        classification = "BREAKING"
        notes.append(f"Format changed '{old_fmt}'→'{new_fmt}' — validation regex changes.")

    if not changes:
        return {
            "column": col,
            "change_type": "no_change",
            "classification": "COMPATIBLE",
            "migration_note": "No changes detected.",
            "details": {},
        }

    return {
        "column": col,
        "change_type": "field_modified",
        "classification": classification,
        "migration_note": " ".join(notes) if notes else "See details.",
        "details": {"changes": changes, "old": old_clause, "new": new_clause},
    }


def diff_snapshots(old_path: str, new_path: str) -> dict:
    """
    Diff two snapshots and return a structured evolution report.
    """
    old = load_snapshot(old_path)
    new = load_snapshot(new_path)

    old_props: dict = old.get("properties", {})
    new_props: dict = new.get("properties", {})

    all_cols = set(old_props) | set(new_props)
    field_changes = []

    for col in sorted(all_cols):
        change = _classify_field_change(
            col,
            old_props.get(col),
            new_props.get(col),
        )
        if change["change_type"] != "no_change":
            field_changes.append(change)

    breaking = [c for c in field_changes if c["classification"] == "BREAKING"]
    compatible = [c for c in field_changes if c["classification"] == "COMPATIBLE"]

    overall = "BREAKING" if breaking else ("COMPATIBLE" if compatible else "UNCHANGED")

    return {
        "contract_id": old.get("contract_id", "unknown"),
        "old_version": old.get("contract_version"),
        "new_version": new.get("contract_version"),
        "old_snapshot_at": old.get("snapshot_at"),
        "new_snapshot_at": new.get("snapshot_at"),
        "overall_classification": overall,
        "total_changes": len(field_changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(compatible),
        "field_changes": field_changes,
        "migration_required": len(breaking) > 0,
        "migration_notes": [c["migration_note"] for c in breaking],
    }


def print_diff_report(diff: dict) -> None:
    print(f"\n[schema_analyzer] Contract : {diff['contract_id']}")
    print(f"[schema_analyzer] Versions : {diff['old_version']} → {diff['new_version']}")
    print(f"[schema_analyzer] Overall  : {diff['overall_classification']}")
    print(f"[schema_analyzer] Changes  : {diff['total_changes']} "
          f"(BREAKING={diff['breaking_changes']}, COMPATIBLE={diff['compatible_changes']})")

    for change in diff["field_changes"]:
        marker = "BREAKING" if change["classification"] == "BREAKING" else "compatible"
        print(f"  [{marker}] {change['column']}: {change['change_type']}")
        print(f"    Note: {change['migration_note']}")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Schema snapshot and evolution analysis.")
    sub = parser.add_subparsers(dest="command")

    snap = sub.add_parser("snapshot", help="Write a timestamped snapshot of a contract")
    snap.add_argument("--contract", required=True)
    snap.add_argument("--snapshots-dir", default="schema_snapshots")

    diff_p = sub.add_parser("diff", help="Diff two contract snapshots")
    diff_p.add_argument("--old", required=True)
    diff_p.add_argument("--new", required=True)
    diff_p.add_argument("--output", default=None, help="Write diff JSON to this path")

    args = parser.parse_args()

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

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
