"""
ReportGenerator — Phase 4B
Auto-generates the Enforcer Report from live validation, attribution, schema
evolution, and AI extension data. Produces a PDF and a machine-readable JSON.

Required sections (per spec):
  1. Data Health Score     — 0–100 with one-sentence narrative
  2. Violations this week  — count by severity + top 3 plain-language descriptions
  3. Schema changes        — plain-language summary of changes in the past 7 days
  4. AI system risk        — embedding drift, prompt input rate, output violation rate
  5. Recommended actions   — 3 specific, prioritised, file-and-field-level actions

Outputs:
  enforcer_report/report_data.json    — machine-readable full data
  enforcer_report/report_{date}.pdf   — stakeholder PDF (requires reportlab)
  enforcer_report/report_{date}.txt   — fallback if reportlab is not installed

Usage:
    python contracts/report_generator.py \
        --violations violation_log/violations.jsonl \
        --reports validation_reports/ \
        --ai-metrics schema_snapshots/ai_baselines.json \
        --evolution validation_reports/ \
        --output enforcer_report/
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )
    _REPORTLAB = True
except ImportError:
    _REPORTLAB = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_jsonl(path: str) -> list[dict]:
    records = []
    p = Path(path)
    if not p.exists():
        return records
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _load_reports(reports_dir: str) -> list[dict]:
    reports = []
    p = Path(reports_dir)
    if not p.exists():
        return reports
    for fp in sorted(p.glob("*.json")):
        try:
            with open(fp, encoding="utf-8") as f:
                data = json.load(f)
            # Only include validation reports (not migration impact / schema evolution)
            if "contract_id" in data and "total_checks" in data:
                reports.append(data)
        except Exception:
            pass
    return reports


def _load_evolution_reports(reports_dir: str) -> list[dict]:
    """Load migration impact and schema evolution reports from a directory."""
    result = []
    p = Path(reports_dir)
    if not p.exists():
        return result
    for fp in sorted(p.glob("migration_impact_*.json")):
        try:
            with open(fp, encoding="utf-8") as f:
                result.append(json.load(f))
        except Exception:
            pass
    for fp in sorted(p.glob("schema_evolution_*.json")):
        try:
            with open(fp, encoding="utf-8") as f:
                result.append(json.load(f))
        except Exception:
            pass
    return result


# ── Section 1: Data Health Score ──────────────────────────────────────────────

_CONTRACT_ID_TO_SYSTEM = {
    "week1": "Roo-Code (Week 1 — Intent Correlator)",
    "week2": "Automaton-Auditor (Week 2 — Digital Courtroom)",
    "week3": "Document Refinery (Week 3 — Extraction Pipeline)",
    "week4": "Codebase-Understanding-Agent (Week 4 — Cartographer)",
    "week5": "The-Ledger (Week 5 — Event Store)",
    "langsmith": "LangSmith AI Traces",
}


def _system_name(contract_id: str) -> str:
    for k, v in _CONTRACT_ID_TO_SYSTEM.items():
        if k in contract_id:
            return v
    return contract_id


def compute_health_score(
    reports: list[dict],
    violations: list[dict],
) -> tuple[float, str]:
    """
    score = (checks_passed / total_checks) × 100 − (critical_violations × 20)
    Clamped to [0, 100]. Returns (score, one_sentence_narrative).
    """
    total_checks = sum(r.get("total_checks", 0) for r in reports)
    passed = sum(r.get("passed", 0) for r in reports)

    if total_checks == 0:
        return 0.0, "No checks have been run yet — deploy contracts and run ValidationRunner."

    base = (passed / total_checks) * 100.0

    # Count CRITICAL violations from violation log
    critical_count = sum(
        1 for v in violations
        if any(
            r.get("severity") == "CRITICAL"
            for r in v.get("results", [{"severity": "HIGH"}])
        )
    )
    # Also count from validation reports directly
    for r in reports:
        for res in r.get("results", []):
            if res.get("severity") == "CRITICAL":
                critical_count += 1

    score = max(0.0, min(100.0, base - critical_count * 20))
    score = round(score, 1)

    if score >= 90:
        narrative = "Data quality is excellent — all monitored systems are within contract bounds."
    elif score >= 70:
        narrative = (
            f"Data quality is good — {critical_count} critical issue(s) require monitoring "
            "but are not currently blocking downstream consumers."
        )
    elif score >= 50:
        narrative = (
            f"Data quality is degraded — {critical_count} CRITICAL violation(s) require "
            "immediate attention from the data engineering team."
        )
    else:
        narrative = (
            f"Data quality is critically compromised — {critical_count} CRITICAL violation(s) "
            "are actively impacting downstream consumers. Escalate immediately."
        )

    return score, narrative


# ── Section 2: Violations this week ──────────────────────────────────────────

_SEVERITY_RANK = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}


def _top_violations(
    violations: list[dict],
    reports: list[dict],
    n: int = 3,
) -> list[dict]:
    """
    Return the n most significant violations, sorted by severity then record count.
    Each entry includes: system name, failing field, downstream impact.
    """
    # Build a flat list from violations.jsonl (attributed violations)
    ranked = sorted(
        violations,
        key=lambda v: (
            _SEVERITY_RANK.get(
                # severity comes from blast_radius or the check results
                max(
                    (r.get("severity", "LOW") for r in v.get("results", [])),
                    key=lambda s: _SEVERITY_RANK.get(s, 0),
                    default="LOW",
                ),
                0,
            ),
            v.get("records_failing", 0),
        ),
        reverse=True,
    )

    result = []
    seen_fields: set[str] = set()

    for v in ranked:
        field = v.get("field", "unknown")
        contract_id = v.get("contract_id", "unknown")
        key = f"{contract_id}:{field}"
        if key in seen_fields:
            continue
        seen_fields.add(key)

        # Downstream impact from blast radius
        blast = v.get("blast_radius", {})
        subs = blast.get("registry_subscribers", [])
        impact_parts = []
        for s in subs[:2]:
            sid = s.get("subscriber_id", "?")
            reason = ""
            for f in s.get("fields_consumed", []):
                if field in str(f):
                    reason = f" (uses {field})"
                    break
            impact_parts.append(f"{sid}{reason}")

        if not impact_parts:
            impact_parts = ["no registered downstream subscribers found"]

        result.append({
            "system": _system_name(contract_id),
            "contract_id": contract_id,
            "field": field,
            "check_type": v.get("check_type", "unknown"),
            "status": v.get("status", "FAIL"),
            "records_failing": v.get("records_failing", 0),
            "message": v.get("message", ""),
            "downstream_impact": ", ".join(impact_parts),
            "top_blame": (
                v["blame_chain"][0]["author"] if v.get("blame_chain") else "unknown"
            ),
        })

        if len(result) >= n:
            break

    return result


def _severity_counts(reports: list[dict], violations: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for r in reports:
        for res in r.get("results", []):
            sev = res.get("severity", "LOW")
            counts[sev] = counts.get(sev, 0) + 1
    return counts


# ── Section 3: Schema changes ─────────────────────────────────────────────────

def _schema_changes_section(evolution_reports: list[dict]) -> list[dict]:
    """Summarise each schema change report into a plain-language entry."""
    entries = []
    for ev in evolution_reports:
        contract_id = ev.get("contract_id", "unknown")
        verdict = ev.get("compatibility_verdict", "UNKNOWN")

        breaking = [
            c for c in ev.get("changes", [])
            if c.get("classification") == "breaking"
        ]
        compatible = [
            c for c in ev.get("changes", [])
            if c.get("classification") == "compatible"
        ]

        # Plain-language summary of breaking changes
        change_summaries = []
        for c in breaking[:3]:
            change_summaries.append(
                f"{c['field']}: {c['change_type'].replace('_', ' ').lower()} — "
                f"{c.get('migration_note', '')[:100]}"
            )

        # Action required per consumer
        actions = []
        for consumer in ev.get("per_consumer_failure_modes", []):
            sid = consumer.get("subscriber_id", "?")
            contact = consumer.get("contact", "")
            actions.append(
                f"Notify {sid}{' (' + contact + ')' if contact else ''}: "
                f"update to handle {[f['field'] for f in consumer.get('failures', [])]}"
            )

        entries.append({
            "contract_id": contract_id,
            "system": _system_name(contract_id),
            "compatibility_verdict": verdict,
            "breaking_count": len(breaking),
            "compatible_count": len(compatible),
            "change_summaries": change_summaries,
            "actions_required": actions[:3],
        })

    return entries


# ── Section 4: AI system risk ─────────────────────────────────────────────────

def _ai_risk_section(ai_metrics: dict) -> dict:
    """Summarise AI extension results into a risk assessment."""
    embedding = ai_metrics.get("embedding_centroid", {})
    output_rate = ai_metrics.get("output_violation_rate", {})

    # Embedding drift
    drift_status = "UNKNOWN"
    drift_detail = "No embedding baseline found."
    if "centroid" in embedding or embedding.get("status"):
        drift_status = embedding.get("status", "PASS")
        drift_detail = (
            f"Cosine distance from baseline: "
            f"{embedding.get('last_drift_score', 'n/a')}"
        )

    # Output violation rate
    rate = output_rate.get("rate", 0)
    rate_status = "PASS" if rate <= 0.02 else "WARN"
    rate_detail = f"Current violation rate: {rate:.2%}" if rate else "No baseline recorded."

    overall = "PASS"
    if drift_status == "FAIL" or rate_status == "WARN":
        overall = "WATCH"
    if drift_status == "FAIL" and rate_status == "WARN":
        overall = "FAIL"

    return {
        "embedding_drift": {"status": drift_status, "detail": drift_detail},
        "output_violation_rate": {"status": rate_status, "detail": rate_detail},
        "overall_ai_status": overall,
        "summary": (
            "AI systems are operating within acceptable bounds."
            if overall == "PASS"
            else "One or more AI extension checks require investigation. See details below."
        ),
    }


# ── Section 5: Recommended actions ───────────────────────────────────────────

def generate_recommended_actions(
    reports: list[dict],
    violations: list[dict],
    evolution_reports: list[dict],
    ai_risk: dict,
) -> list[dict]:
    """
    Generate 3 prioritised, specific recommended actions for the data engineering team.
    Each action names the exact file, field, and contract clause where applicable.
    Ordered by risk reduction value: CRITICAL > breaking schema change > AI warning.
    """
    actions: list[dict] = []

    # Priority 1 — Most severe violation with blame chain
    critical_violations = [
        v for v in violations
        if any(
            r.get("severity") in ("CRITICAL", "HIGH")
            for r in v.get("results", [{"severity": "HIGH"}])
        )
    ]
    if critical_violations:
        v = critical_violations[0]
        blame = v.get("blame_chain", [{}])[0]
        file_path = blame.get("file_path", "unknown file")
        field = v.get("field", "unknown field")
        cid = v.get("contract_id", "contract")
        check = v.get("check_type", "check")

        # Map field to JSON path notation for the clause reference
        clause_ref = field.replace("[*]", "")
        actions.append({
            "priority": 1,
            "risk_level": "CRITICAL",
            "action": (
                f"Fix {file_path}: update the producer to output '{field}' "
                f"within the bounds defined by contract {cid} clause {clause_ref}.{check}. "
                f"Attributed to: {blame.get('author', 'unknown')} "
                f"(commit {blame.get('commit_hash', 'unknown')[:8]})."
            ),
            "contract_id": cid,
            "field": field,
        })

    # Priority 2 — Breaking schema change (migration required)
    breaking_evolutions = [
        e for e in evolution_reports
        if e.get("compatibility_verdict") == "BREAKING"
    ]
    if breaking_evolutions:
        ev = breaking_evolutions[0]
        cid = ev.get("contract_id", "contract")
        first_change = next(
            (c for c in ev.get("changes", []) if c.get("classification") == "breaking"),
            None,
        )
        if first_change:
            field = first_change["field"]
            change_type = first_change["change_type"]
            note = first_change.get("migration_note", "")[:120]
            # Get first affected consumer contact
            consumers = ev.get("per_consumer_failure_modes", [])
            contact = consumers[0].get("contact", "") if consumers else ""
            subscriber = consumers[0].get("subscriber_id", "") if consumers else ""
            actions.append({
                "priority": 2,
                "risk_level": "HIGH",
                "action": (
                    f"Run migration for {cid}: field '{field}' has a BREAKING "
                    f"{change_type.replace('_', ' ').lower()} change. {note}. "
                    + (
                        f"Coordinate with {subscriber}{' (' + contact + ')' if contact else ''} "
                        "before deploying."
                        if subscriber else "Update all registered consumers before deploying."
                    )
                ),
                "contract_id": cid,
                "field": field,
            })

    # Priority 3 — AI risk or remaining violation
    if ai_risk.get("overall_ai_status") in ("WATCH", "FAIL"):
        drift = ai_risk.get("embedding_drift", {})
        rate = ai_risk.get("output_violation_rate", {})
        if drift.get("status") == "FAIL":
            actions.append({
                "priority": 3,
                "risk_level": "MEDIUM",
                "action": (
                    "Investigate embedding drift in extracted_facts[*].text: "
                    f"{drift.get('detail', '')}. "
                    "Check if the document domain or extraction prompt has changed. "
                    "Delete schema_snapshots/ai_baselines/*_centroid.npz after the "
                    "domain shift is confirmed and re-establish the baseline."
                ),
                "contract_id": "week3_extractions",
                "field": "extracted_facts.text",
            })
        elif rate.get("status") == "WARN":
            actions.append({
                "priority": 3,
                "risk_level": "MEDIUM",
                "action": (
                    "Investigate rising LLM output schema violation rate: "
                    f"{rate.get('detail', '')}. "
                    "Verify that overall_verdict ∈ {PASS, FAIL, WARN} is still "
                    "enforced in the Week 2 verdict prompt. Check for recent model "
                    "version rollout or prompt template changes."
                ),
                "contract_id": "week2_verdicts",
                "field": "overall_verdict",
            })
    elif len(violations) > 0 and len(actions) < 3:
        # Use second-most-severe violation as priority 3
        remaining = [v for v in violations if v not in critical_violations[:1]]
        if remaining:
            v = remaining[0]
            field = v.get("field", "unknown")
            cid = v.get("contract_id", "contract")
            actions.append({
                "priority": 3,
                "risk_level": "LOW",
                "action": (
                    f"Monitor {cid}: field '{field}' has a non-critical violation. "
                    f"{v.get('message', '')[:120]} "
                    "Add to next sprint backlog."
                ),
                "contract_id": cid,
                "field": field,
            })

    # Pad to 3 actions
    if not actions:
        actions.append({
            "priority": 1,
            "risk_level": "INFO",
            "action": "No violations detected. Run all contracts on a schedule to maintain baselines.",
            "contract_id": "all",
            "field": "n/a",
        })
    while len(actions) < 3:
        actions.append({
            "priority": len(actions) + 1,
            "risk_level": "INFO",
            "action": (
                "Continue monitoring. Schedule weekly contract runs to catch "
                "drift before it reaches downstream consumers."
            ),
            "contract_id": "all",
            "field": "n/a",
        })

    return actions[:3]


# ── PDF generation ────────────────────────────────────────────────────────────

def _write_pdf_report(report_data: dict, output_path: Path) -> None:
    """
    Generate the Enforcer Report PDF using reportlab.
    Falls back to a warning print if reportlab is unavailable
    (caller should already have written the .txt version).
    """
    if not _REPORTLAB:
        print(
            "[report_generator] reportlab not installed — PDF not generated. "
            "Install with: pip install reportlab"
        )
        return

    GREEN = colors.HexColor("#2ECC71")
    DARK = colors.HexColor("#1A1A2E")
    WARN_COLOR = colors.HexColor("#E74C3C")
    LIGHT_GRAY = colors.HexColor("#F5F5F5")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "title", parent=styles["Title"],
        fontSize=22, textColor=DARK, spaceAfter=6,
    )
    heading_style = ParagraphStyle(
        "heading", parent=styles["Heading2"],
        fontSize=13, textColor=DARK, spaceBefore=14, spaceAfter=4,
        borderPad=4,
    )
    body_style = ParagraphStyle(
        "body", parent=styles["Normal"],
        fontSize=9, spaceAfter=4, leading=13,
    )
    small_style = ParagraphStyle(
        "small", parent=styles["Normal"],
        fontSize=8, textColor=colors.HexColor("#555555"),
    )
    score_style = ParagraphStyle(
        "score", parent=styles["Normal"],
        fontSize=48, textColor=GREEN, spaceAfter=0, leading=52,
    )

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=2 * cm, leftMargin=2 * cm,
        topMargin=2 * cm, bottomMargin=2 * cm,
    )
    story = []

    def hr():
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#E0E0E0")))
        story.append(Spacer(1, 8))

    # ── Cover ──
    story.append(Paragraph("DATA CONTRACT ENFORCER", title_style))
    story.append(Paragraph("Enforcement Report", heading_style))
    story.append(Paragraph(
        f"Generated: {report_data['generated_at']}  |  "
        f"Contracts checked: {report_data['summary']['contracts_checked']}  |  "
        f"Total checks: {report_data['summary']['total_checks']}",
        small_style,
    ))
    story.append(Spacer(1, 12))
    hr()

    # ── Section 1: Health Score ──
    story.append(Paragraph("1. DATA HEALTH SCORE", heading_style))
    score = report_data["health_score"]["score"]
    narrative = report_data["health_score"]["narrative"]
    score_color = GREEN if score >= 70 else WARN_COLOR
    story.append(Paragraph(
        f'<font color="{score_color.hexval() if hasattr(score_color, "hexval") else "#2ECC71"}">'
        f'{score}</font> / 100',
        score_style,
    ))
    story.append(Paragraph(narrative, body_style))
    story.append(Spacer(1, 8))
    hr()

    # ── Section 2: Violations ──
    story.append(Paragraph("2. VIOLATIONS THIS WEEK", heading_style))
    sev = report_data["summary"]
    severity_rows = [
        ["Severity", "Count"],
        ["CRITICAL", str(sev.get("critical", 0))],
        ["HIGH", str(sev.get("high", 0))],
        ["MEDIUM", str(sev.get("medium", 0))],
        ["LOW", str(sev.get("low", 0))],
    ]
    t = Table(severity_rows, colWidths=[6 * cm, 3 * cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), DARK),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
        ("ALIGN", (1, 0), (1, -1), "CENTER"),
    ]))
    story.append(t)
    story.append(Spacer(1, 10))

    for i, v in enumerate(report_data.get("top_violations", []), 1):
        story.append(Paragraph(
            f"<b>{i}. [{v['status']}] {v['system']} — {v['field']}</b>", body_style
        ))
        story.append(Paragraph(f"   {v['message']}", body_style))
        story.append(Paragraph(
            f"   <i>Downstream impact: {v['downstream_impact']}</i>", small_style
        ))
        story.append(Spacer(1, 4))
    hr()

    # ── Section 3: Schema Changes ──
    story.append(Paragraph("3. SCHEMA CHANGES DETECTED (LAST 7 DAYS)", heading_style))
    schema_changes = report_data.get("schema_changes", [])
    if not schema_changes:
        story.append(Paragraph("No schema changes detected in the past 7 days.", body_style))
    for sc in schema_changes:
        verdict_color = "#E74C3C" if sc["compatibility_verdict"] == "BREAKING" else "#2ECC71"
        story.append(Paragraph(
            f"<b>{sc['system']}</b> — "
            f'<font color="{verdict_color}">{sc["compatibility_verdict"]}</font> '
            f"({sc['breaking_count']} breaking, {sc['compatible_count']} compatible)",
            body_style,
        ))
        for chg in sc.get("change_summaries", []):
            story.append(Paragraph(f"  • {chg}", small_style))
        for act in sc.get("actions_required", []):
            story.append(Paragraph(f"  → {act}", small_style))
        story.append(Spacer(1, 4))
    hr()

    # ── Section 4: AI Risk ──
    story.append(Paragraph("4. AI SYSTEM RISK ASSESSMENT", heading_style))
    ai = report_data.get("ai_risk", {})
    ai_rows = [
        ["Check", "Status", "Detail"],
        ["Embedding Drift",
         ai.get("embedding_drift", {}).get("status", "UNKNOWN"),
         ai.get("embedding_drift", {}).get("detail", "")[:60]],
        ["Prompt Input Validation",
         ai.get("prompt_input", {}).get("status", "SKIP"),
         ai.get("prompt_input", {}).get("detail", "")[:60]],
        ["LLM Output Violation Rate",
         ai.get("output_violation_rate", {}).get("status", "UNKNOWN"),
         ai.get("output_violation_rate", {}).get("detail", "")[:60]],
    ]
    t2 = Table(ai_rows, colWidths=[5 * cm, 3 * cm, 9 * cm])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), DARK),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
    ]))
    story.append(t2)
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        f"Overall AI Status: <b>{ai.get('overall_ai_status', 'UNKNOWN')}</b>  — "
        f"{ai.get('summary', '')}",
        body_style,
    ))
    hr()

    # ── Section 5: Recommended Actions ──
    story.append(Paragraph("5. RECOMMENDED ACTIONS", heading_style))
    story.append(Paragraph(
        "Ordered by risk reduction value. Each action is specific to a file, "
        "field, and contract clause.",
        small_style,
    ))
    story.append(Spacer(1, 6))
    for action in report_data.get("recommended_actions", []):
        risk = action.get("risk_level", "")
        risk_color = (
            "#E74C3C" if risk == "CRITICAL" else
            "#E67E22" if risk == "HIGH" else
            "#F39C12" if risk == "MEDIUM" else "#27AE60"
        )
        story.append(Paragraph(
            f'<b>{action["priority"]}. '
            f'[<font color="{risk_color}">{risk}</font>]</b> '
            f'{action["action"]}',
            body_style,
        ))
        story.append(Spacer(1, 6))

    doc.build(story)
    print(f"[report_generator] PDF report      : {output_path}")


# ── Text fallback ─────────────────────────────────────────────────────────────

def _write_text_report(report_data: dict, output_path: Path) -> None:
    """Plain-text report — always written regardless of reportlab availability."""
    s = report_data["summary"]
    hs = report_data["health_score"]
    lines = [
        "=" * 72,
        "  DATA CONTRACT ENFORCER — ENFORCEMENT REPORT",
        f"  Generated: {report_data['generated_at']}",
        "=" * 72,
        "",
        "1. DATA HEALTH SCORE",
        "--------------------",
        f"  Score     : {hs['score']} / 100",
        f"  Narrative : {hs['narrative']}",
        "",
        "2. VIOLATIONS THIS WEEK",
        "-----------------------",
        f"  CRITICAL: {s.get('critical', 0)}  |  HIGH: {s.get('high', 0)}  |  "
        f"MEDIUM: {s.get('medium', 0)}  |  LOW: {s.get('low', 0)}",
        "",
    ]
    for i, v in enumerate(report_data.get("top_violations", []), 1):
        lines.append(f"  {i}. [{v['status']}] {v['system']} / {v['field']}")
        lines.append(f"     {v['message'][:120]}")
        lines.append(f"     Downstream: {v['downstream_impact']}")
        lines.append("")

    lines += ["3. SCHEMA CHANGES (LAST 7 DAYS)", "-------------------------------"]
    schema_changes = report_data.get("schema_changes", [])
    if not schema_changes:
        lines.append("  No schema changes detected.")
    for sc in schema_changes:
        lines.append(
            f"  {sc['system']}: {sc['compatibility_verdict']} "
            f"({sc['breaking_count']} breaking)"
        )
        for chg in sc.get("change_summaries", []):
            lines.append(f"    • {chg}")
        for act in sc.get("actions_required", []):
            lines.append(f"    → {act}")
    lines.append("")

    ai = report_data.get("ai_risk", {})
    lines += [
        "4. AI SYSTEM RISK ASSESSMENT",
        "----------------------------",
        f"  Embedding Drift       : {ai.get('embedding_drift', {}).get('status', 'UNKNOWN')}  "
        f"— {ai.get('embedding_drift', {}).get('detail', '')}",
        f"  Prompt Input Valid.   : {ai.get('prompt_input', {}).get('status', 'SKIP')}  "
        f"— {ai.get('prompt_input', {}).get('detail', '')}",
        f"  Output Violation Rate : {ai.get('output_violation_rate', {}).get('status', 'UNKNOWN')}  "
        f"— {ai.get('output_violation_rate', {}).get('detail', '')}",
        f"  Overall AI Status     : {ai.get('overall_ai_status', 'UNKNOWN')}",
        f"  {ai.get('summary', '')}",
        "",
        "5. RECOMMENDED ACTIONS",
        "----------------------",
    ]
    for action in report_data.get("recommended_actions", []):
        lines.append(
            f"  {action['priority']}. [{action['risk_level']}] {action['action']}"
        )
        lines.append("")

    lines += ["=" * 72, ""]
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[report_generator] Text report     : {output_path}")


# ── Main report assembly ──────────────────────────────────────────────────────

def generate_report(
    violations_path: str = "violation_log/violations.jsonl",
    reports_dir: str = "validation_reports/",
    ai_metrics_path: str = "schema_snapshots/ai_baselines.json",
    evolution_dir: str = "validation_reports/",
    output_dir: str = "enforcer_report/",
) -> dict:
    violations = _load_jsonl(violations_path)
    reports = _load_reports(reports_dir)
    evolution_reports = _load_evolution_reports(evolution_dir)

    ai_metrics: dict = {}
    if Path(ai_metrics_path).exists():
        with open(ai_metrics_path, encoding="utf-8") as f:
            ai_metrics = json.load(f)

    # Section 1: Health score
    health_score, health_narrative = compute_health_score(reports, violations)

    # Section 2: Violations
    sev_counts = _severity_counts(reports, violations)
    top_violations = _top_violations(violations, reports)

    # Section 3: Schema changes
    schema_changes = _schema_changes_section(evolution_reports)

    # Section 4: AI risk
    ai_risk = _ai_risk_section(ai_metrics)

    # Section 5: Recommended actions
    recommended_actions = generate_recommended_actions(
        reports, violations, evolution_reports, ai_risk
    )

    # Contracts checked
    contracts_checked = sorted({r.get("contract_id", "unknown") for r in reports})

    report_data: dict = {
        "report_id": str(uuid.uuid4()),
        "generated_at": _now(),
        "health_score": {
            "score": health_score,
            "narrative": health_narrative,
        },
        "summary": {
            "contracts_checked": len(contracts_checked),
            "contract_ids": contracts_checked,
            "total_checks": sum(r.get("total_checks", 0) for r in reports),
            "passed": sum(r.get("passed", 0) for r in reports),
            "failed": sum(r.get("failed", 0) for r in reports),
            "warned": sum(r.get("warned", 0) for r in reports),
            "errored": sum(r.get("errored", 0) for r in reports),
            "critical": sev_counts.get("CRITICAL", 0),
            "high": sev_counts.get("HIGH", 0),
            "medium": sev_counts.get("MEDIUM", 0),
            "low": sev_counts.get("LOW", 0),
            "violations_attributed": len(violations),
        },
        "top_violations": top_violations,
        "schema_changes": schema_changes,
        "ai_risk": ai_risk,
        "recommended_actions": recommended_actions,
        "validation_reports": [
            {
                "report_id": r.get("report_id"),
                "contract_id": r.get("contract_id"),
                "run_timestamp": r.get("run_timestamp"),
                "overall_status": r.get("overall_status"),
                "total_checks": r.get("total_checks"),
                "failed": r.get("failed"),
                "warned": r.get("warned"),
            }
            for r in reports
        ],
    }

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Write machine-readable JSON
    json_path = Path(output_dir) / "report_data.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)
    print(f"[report_generator] Report data     : {json_path}")

    # Write text report (always)
    date_str = datetime.now().strftime("%Y%m%d")
    txt_path = Path(output_dir) / f"report_{date_str}.txt"
    _write_text_report(report_data, txt_path)

    # Write PDF (requires reportlab)
    pdf_path = Path(output_dir) / f"report_{date_str}.pdf"
    _write_pdf_report(report_data, pdf_path)

    return report_data


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate the Enforcer Report (PDF + JSON + TXT)."
    )
    parser.add_argument("--violations", default="violation_log/violations.jsonl")
    parser.add_argument("--reports", default="validation_reports/")
    parser.add_argument("--ai-metrics", default="schema_snapshots/ai_baselines.json")
    parser.add_argument("--evolution", default="validation_reports/",
                        help="Directory containing migration_impact_*.json files")
    parser.add_argument("--output", default="enforcer_report/")
    args = parser.parse_args()

    report = generate_report(
        violations_path=args.violations,
        reports_dir=args.reports,
        ai_metrics_path=args.ai_metrics,
        evolution_dir=args.evolution,
        output_dir=args.output,
    )

    s = report["summary"]
    hs = report["health_score"]
    print(
        f"\n[report_generator] Health score    : {hs['score']}/100 — {hs['narrative'][:60]}..."
    )
    print(
        f"[report_generator] Summary         : {s['contracts_checked']} contracts, "
        f"{s['total_checks']} checks, {s['failed']} failed, "
        f"{s['violations_attributed']} violations attributed."
    )
    print("[report_generator] Recommended actions:")
    for a in report["recommended_actions"]:
        print(f"  {a['priority']}. [{a['risk_level']}] {a['action'][:100]}...")


if __name__ == "__main__":
    main()
