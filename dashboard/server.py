"""
Dashboard server — serves the Enforcer dashboard and all report data as JSON APIs.
Automatically discovers contracts, validation reports, and baselines from disk.

Run:
    python dashboard/server.py
Then open: http://localhost:5050
"""

import json
import os
import sys
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, jsonify, send_from_directory

app = Flask(__name__, static_folder=".", static_url_path="")

ROOT = Path(__file__).parent.parent
CONTRACTS_DIR = ROOT / "generated_contracts"
REPORTS_DIR = ROOT / "validation_reports"
BASELINES_FILE = ROOT / "schema_snapshots" / "baselines.json"
VIOLATION_LOG = ROOT / "violation_log"
ENFORCER_REPORT_DIR = ROOT / "enforcer_report"


# ── Static files ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(Path(__file__).parent), "index.html")


# ── API: Contracts ────────────────────────────────────────────────────────────

@app.route("/api/contracts")
def list_contracts():
    contracts = []
    if CONTRACTS_DIR.exists():
        import yaml
        for f in sorted(CONTRACTS_DIR.glob("*.yaml")):
            try:
                with open(f, encoding="utf-8") as fh:
                    c = yaml.safe_load(fh)
                props = c.get("schema", {}).get("properties", {})
                contracts.append({
                    "id": c.get("id", f.stem),
                    "version": c.get("version", "—"),
                    "description": c.get("description", ""),
                    "record_count": c.get("source", {}).get("record_count", "—"),
                    "generated_at": c.get("source", {}).get("generated_at", "—"),
                    "field_count": len(props),
                    "fields": list(props.keys()),
                    "quality_checks": len(c.get("quality", {}).get("checks", [])),
                    "lineage": c.get("lineage", {}),
                    "filename": f.name,
                })
            except Exception as e:
                contracts.append({"id": f.stem, "error": str(e)})
    return jsonify(contracts)


@app.route("/api/contracts/<contract_id>")
def get_contract(contract_id):
    import yaml
    path = CONTRACTS_DIR / f"{contract_id}.yaml"
    if not path.exists():
        return jsonify({"error": "Not found"}), 404
    with open(path, encoding="utf-8") as f:
        return jsonify(yaml.safe_load(f))


# ── API: Validation Reports ───────────────────────────────────────────────────

@app.route("/api/reports")
def list_reports():
    reports = []
    if REPORTS_DIR.exists():
        for f in sorted(REPORTS_DIR.glob("*.json"), reverse=True):
            try:
                with open(f, encoding="utf-8") as fh:
                    r = json.load(fh)
                reports.append({
                    "filename": f.name,
                    "contract_id": r.get("contract_id", "—"),
                    "overall_status": r.get("overall_status", "—"),
                    "record_count": r.get("record_count", "—"),
                    "violation_count": r.get("violation_count", 0),
                    "summary": r.get("summary", {}),
                    "validated_at": r.get("validated_at", "—"),
                    "data_path": r.get("data_path", "—"),
                })
            except Exception as e:
                reports.append({"filename": f.name, "error": str(e)})
    return jsonify(reports)


@app.route("/api/reports/<filename>")
def get_report(filename):
    path = REPORTS_DIR / filename
    if not path.exists():
        return jsonify({"error": "Not found"}), 404
    with open(path, encoding="utf-8") as f:
        return jsonify(json.load(f))


# ── API: Baselines ────────────────────────────────────────────────────────────

@app.route("/api/baselines")
def get_baselines():
    if not BASELINES_FILE.exists():
        return jsonify({"written_at": None, "columns": {}})
    with open(BASELINES_FILE, encoding="utf-8") as f:
        return jsonify(json.load(f))


# ── API: Summary (dashboard home stats) ──────────────────────────────────────

@app.route("/api/summary")
def get_summary():
    import yaml

    n_contracts = len(list(CONTRACTS_DIR.glob("*.yaml"))) if CONTRACTS_DIR.exists() else 0

    reports = []
    if REPORTS_DIR.exists():
        for f in REPORTS_DIR.glob("*.json"):
            try:
                with open(f, encoding="utf-8") as fh:
                    reports.append(json.load(fh))
            except Exception:
                pass

    n_reports = len(reports)
    status_counts = {"PASS": 0, "WARN": 0, "FAIL": 0}
    total_violations = 0
    for r in reports:
        s = r.get("overall_status", "")
        if s in status_counts:
            status_counts[s] += 1
        total_violations += r.get("violation_count", 0)

    baselines_columns = 0
    if BASELINES_FILE.exists():
        with open(BASELINES_FILE, encoding="utf-8") as f:
            baselines_columns = len(json.load(f).get("columns", {}))

    return jsonify({
        "contracts": n_contracts,
        "reports": n_reports,
        "status_counts": status_counts,
        "total_violations": total_violations,
        "baselined_columns": baselines_columns,
        "phase": 1,
    })


# ── API: Enforcer Report (Phase 3+) ──────────────────────────────────────────

@app.route("/api/enforcer-report")
def get_enforcer_report():
    if not ENFORCER_REPORT_DIR.exists():
        return jsonify({"available": False, "message": "Enforcer Report not yet generated (Phase 3)."})
    reports = list(ENFORCER_REPORT_DIR.glob("*.json"))
    if not reports:
        return jsonify({"available": False, "message": "No Enforcer Report found yet."})
    latest = max(reports, key=lambda p: p.stat().st_mtime)
    with open(latest, encoding="utf-8") as f:
        data = json.load(f)
    data["available"] = True
    return jsonify(data)


if __name__ == "__main__":
    print("Data Contract Enforcer Dashboard")
    print("Open http://localhost:5050 in your browser")
    app.run(port=5050, debug=True, use_reloader=False)
