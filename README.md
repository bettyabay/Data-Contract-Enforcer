# Data Contract Enforcer

A full-stack data contract enforcement system built on the [Bitol Open Data Contract Standard v3.0.0](https://bitol-io.github.io/open-data-contract-standard/). Covers contract generation, violation detection, blame attribution, schema evolution analysis, AI-specific contract checks, and automated enforcement reporting — across a six-system agentic platform.

---

## What It Does

| Phase | Module | What It Produces |
|-------|--------|-----------------|
| 0 — Generate | `contracts/generator.py` | Bitol YAML contract auto-generated from JSONL source data, LLM-annotated |
| 1 — Validate | `contracts/runner.py` | Structured JSON validation report; exit 1 on violations in ENFORCE mode |
| 2 — Attribute | `contracts/attributor.py` | Blame chain with lineage BFS + git blame; blast radius from contract registry |
| 3 — Evolve | `contracts/schema_analyzer.py` | Schema diff with Bitol change taxonomy; migration impact report |
| 4A — AI Checks | `contracts/ai_extensions.py` | Embedding drift, prompt input schema, LLM output violation rate |
| 4B — Report | `contracts/report_generator.py` | Health score, top violations, schema changes, AI risk — PDF + JSON + TXT |

---

## Platform Architecture

```
Week 1 (Intent Classifier)
  └─ week1_intent_records ────────────────────────────────────────────────────────┐
                                                                                   ▼
Week 2 (Automaton-Auditor / Digital Courtroom)                          Week 7 ── Data Contract Enforcer
  └─ week2_verdicts ──────────────────────────────────────────────────────────────┤
                                                                                   │
Week 3 (Layout-Aware Document Refinery)                                            │
  └─ week3_extractions ──► Week 4 (Codebase-Understanding-Agent)  ────────────────┤
       │                        └─ week4_lineage ────────────────────────────────► │
       └─ CRITICAL violation: fact_confidence scale change (0-1 → 0-100)           │
                                                                                   │
Week 5 (The Ledger — Agentic Event Store)                                          │
  └─ week5_events ───────────────────────────────────────────────────────────────► ┘

LangSmith (Observability)
  └─ langsmith_traces ─────────────────────────────────────────────────── AI Checks
```

All inter-system dependencies are formally registered in `contract_registry/subscriptions.yaml`.

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

Requires Python 3.12+. Set `ANTHROPIC_API_KEY` in a `.env` file for LLM contract annotation (supports both native Anthropic keys and OpenRouter keys prefixed `sk-or-v1-`).

```
# .env
ANTHROPIC_API_KEY=your-key-here
```

### 2. Run the full pipeline against Week 3

```bash
# Generate contract
py -3.13 -m contracts.generator \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3_extractions \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/

# Validate violated batch (ENFORCE mode)
py -3.13 -m contracts.runner \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json \
  --baselines validation_reports/week3_extractions_baseline.json \
  --mode ENFORCE

# Attribute blame + blast radius
py -3.13 -m contracts.attributor \
  --report validation_reports/week3_violated.json \
  --lineage "path/to/Codebase-Understanding-Agent/outputs/week4/lineage_snapshots.jsonl" \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl

# Schema evolution diff
py -3.13 -m contracts.schema_analyzer diff \
  --old schema_snapshots/week3_extractions/20260404_153046_16eb3cf9.json \
  --new schema_snapshots/week3_extractions/20260404_153500_broken.json \
  --output validation_reports/schema_evolution_week3.json

# AI contract extensions
py -3.13 -m contracts.ai_extensions \
  --traces outputs/traces/runs.jsonl \
  --contract generated_contracts/langsmith_traces.yaml \
  --baselines validation_reports/langsmith_traces_baseline.json \
  --output validation_reports/ai_extensions.json

# Enforcer report (PDF + JSON + TXT)
py -3.13 -m contracts.report_generator \
  --violations violation_log/violations.jsonl \
  --reports validation_reports/ \
  --ai-metrics validation_reports/ai_extensions.json \
  --evolution validation_reports/ \
  --output enforcer_report/
```

---

## Modules

### `contracts/generator.py` — Contract Generator

Profiles a JSONL source file and generates a Bitol v3.0.0 YAML contract. Uses `pandas` for field profiling (types, ranges, enums, null rates, formats) and calls Claude via the Anthropic API (or OpenRouter) to annotate semantically ambiguous columns.

**Key outputs per field:**
- Type inference (`string`, `integer`, `number`, `boolean`)
- Required / nullable flag
- Range bounds (`minimum`, `maximum`) for numerics
- Enum values for low-cardinality strings
- UUID format + pattern for ID fields
- `statistical_drift` checks at 2σ (WARN) and 3σ (FAIL)
- LLM-generated `description` for ambiguous columns

Also writes a schema snapshot to `schema_snapshots/<contract_id>/` for evolution tracking and a dbt schema YAML to the output directory.

```
usage: generator.py --source PATH --contract-id ID [--registry PATH] [--output DIR]
```

---

### `contracts/runner.py` — Validation Runner

Validates a JSONL data file against a Bitol contract. Runs every check clause defined in the contract schema and quality block.

**Check types supported:**
- `not_null` — required field presence
- `unique` — duplicate detection
- `range` — min/max bounds (detects confidence scale changes: max > 1.0 when contract says ≤ 1.0)
- `enum` — allowed value set
- `format` — date-time, uuid
- `pattern` — regex match
- `statistical_drift` — z-score against baseline (z > 3 = FAIL/HIGH, z 2–3 = WARN/MEDIUM)

**Enforcement modes:**

| Mode | Behaviour |
|------|-----------|
| `AUDIT` | Log violations, always exit 0 |
| `WARN` | Exit 1 on CRITICAL severity |
| `ENFORCE` | Exit 1 on CRITICAL or HIGH severity |

```
usage: runner.py --contract PATH --data PATH --output PATH [--baselines PATH] [--mode AUDIT|WARN|ENFORCE]
```

---

### `contracts/attributor.py` — Violation Attributor

Attributes each violation in a validation report to a source commit and maps the downstream blast radius. Runs a 4-step pipeline:

1. **Registry lookup** — queries `subscriptions.yaml` for all downstream consumers of the failing field
2. **Lineage BFS** — traverses the Week 4 lineage graph upstream from the violation node to find the originating producer
3. **Git blame** — runs `git blame -L --porcelain` on the upstream repository root (from `codebase_root` in the lineage snapshot)
4. **Write violation log** — appends structured JSONL to `violation_log/violations.jsonl`

Confidence scoring: 0.01 (no git history) → 0.5 (lineage node found) → 0.8 (git commit found) → 0.85+ (blame corroborated).

```
usage: attributor.py --report PATH --lineage PATH [--registry PATH] [--output PATH]
```

---

### `contracts/schema_analyzer.py` — Schema Evolution Analyzer

Diffs two contract snapshots and classifies every field change using the Bitol change taxonomy. Detects probable field renames.

**Change taxonomy:**

| Type | Class | Description |
|------|-------|-------------|
| `add_nullable` | COMPATIBLE | New optional field — backward safe |
| `add_required` | BREAKING | New required field — existing data fails |
| `remove_field` | BREAKING | Field deleted — joins, lookups break |
| `rename` | BREAKING | Field renamed — requires 2-sprint alias |
| `widen_type` | COMPATIBLE | Type widened (int → float) |
| `narrow_type` | BREAKING | **Confidence scale change (0–1 → 0–100) detected** |
| `range_widen` | COMPATIBLE | Bounds relaxed |
| `range_narrow` | BREAKING | Bounds tightened |
| `change_enum_add` | COMPATIBLE | New enum value added |
| `change_enum_remove` | BREAKING | Enum value removed |

The confidence scale change detection is a special case: when `old_max ≤ 1.0` and `new_max ≥ 50`, the change is forced to `BREAKING / narrow_type` regardless of numeric direction.

**Two operating modes:**

```bash
# Phase 3 — auto-diff most recent two snapshots for a contract
py -3.13 -m contracts.schema_analyzer \
  --contract-id week3_extractions --since "7 days ago" \
  --output validation_reports/schema_evolution_week3.json

# Legacy — explicit snapshot pair
py -3.13 -m contracts.schema_analyzer diff \
  --old schema_snapshots/week3_extractions/20260404_153046_16eb3cf9.json \
  --new schema_snapshots/week3_extractions/20260404_153500_broken.json
```

---

### `contracts/ai_extensions.py` — AI Contract Extensions

Runs three contract checks specific to LLM-powered systems, beyond what standard data contracts cover.

**Check 1 — Embedding Drift**
Embeds trace input texts using `sentence-transformers/all-MiniLM-L6-v2` (384-dim, local, no API key). Computes cosine distance between the current centroid and the stored baseline. Threshold: > 0.15 = WARN/FAIL.

**Check 2 — Prompt Input Schema Validation**
Validates every trace's `inputs` object against a JSON schema requiring `inputs.prompt` (≥ 10 chars) and `inputs.context`. Invalid records are quarantined to `outputs/quarantine/`.

**Check 3 — LLM Output Violation Rate**
Checks that `outputs.result` is present in every trace. Computes the violation rate against the contract's allowed output schema. Thresholds: > 2% = WARN, > 5% = FAIL.

Baselines are stored in `schema_snapshots/ai_baselines.json` and updated on each run.

```
usage: ai_extensions.py --traces PATH --contract PATH [--baselines PATH] [--output PATH]
```

---

### `contracts/report_generator.py` — Enforcer Report

Aggregates all contract runs, violation log, schema evolution results, and AI metrics into a single weekly enforcement report.

**Health score formula:**
```
score = (passed / total_checks) * 100 - (critical_count * 20)
score = clamp(score, 0, 100)
```

**Outputs:**
- `enforcer_report/report_data.json` — machine-readable full report
- `enforcer_report/report_<date>.txt` — plain text (always written, no extra deps)
- `enforcer_report/report_<date>.pdf` — formatted PDF via ReportLab

```
usage: report_generator.py [--violations PATH] [--reports DIR]
                            [--ai-metrics PATH] [--evolution DIR] [--output DIR]
```

---

## Contract Registry

`contract_registry/subscriptions.yaml` is the authoritative blast-radius ledger. Every inter-system data dependency is registered here. Each subscription declares:

- `contract_id` — the producing system's contract
- `subscriber_id` / `subscriber_team` — the consuming system
- `fields_consumed` — which fields the consumer reads
- `breaking_fields` — fields whose change would break the consumer, with reasons
- `validation_mode` — `ENFORCE`, `WARN`, or `AUDIT`

The attributor queries this registry first (before the lineage graph) to identify downstream impact. This is the **primary blast radius source** — the lineage graph enriches it but does not replace it.

**Registered subscriptions:**

| Producer | Consumer | Mode | Key Breaking Field |
|----------|----------|------|--------------------|
| `week1_intent_records` | `week2-courtroom` | ENFORCE | `code_refs.confidence` (scale) |
| `week2_verdicts` | `week7-enforcer` | AUDIT | `overall_verdict` (enum) |
| `week3_extractions` | `week4-cartographer` | **ENFORCE** | `extracted_facts.confidence` (scale) |
| `week3_extractions` | `week7-enforcer` | AUDIT | `extracted_facts.confidence` |
| `week4_lineage` | `week7-enforcer` | ENFORCE | `nodes`, `edges` (BFS structure) |
| `week5_events` | `week7-enforcer` | ENFORCE | `event_type`, `sequence_number` |
| `langsmith_traces` | `week7-enforcer` | AUDIT | `run_id`, `total_tokens` |

---

## Repository Structure

```
Data-Contract-Enforcer/
├── contracts/
│   ├── generator.py          # Phase 0: JSONL → Bitol YAML contract
│   ├── runner.py             # Phase 1: Validate data against contract
│   ├── attributor.py         # Phase 2: Blame chain + blast radius
│   ├── schema_analyzer.py    # Phase 3: Schema diff + migration impact
│   ├── ai_extensions.py      # Phase 4A: Embedding drift, prompt/output validation
│   └── report_generator.py   # Phase 4B: PDF/JSON/TXT enforcer report
│
├── contract_registry/
│   └── subscriptions.yaml    # Authoritative inter-system dependency registry
│
├── generated_contracts/
│   ├── week3_extractions.yaml          # Bitol v3.0.0 contract
│   ├── week3_extractions_dbt.yml       # dbt schema (generated alongside)
│   └── ...                             # one YAML per system
│
├── schema_snapshots/
│   ├── ai_baselines.json               # Embedding drift baselines
│   └── week3_extractions/
│       ├── 20260404_152501_*.json      # Clean snapshot (baseline)
│       ├── 20260404_153046_*.json      # Pre-breaking snapshot
│       └── 20260404_153500_broken.json # Synthetic breaking snapshot (demo)
│
├── validation_reports/
│   ├── week3_extractions_baseline.json # Clean-run baseline report
│   ├── week3_violated.json             # (generated on demo run)
│   └── *_baseline.json                 # One baseline per contract
│
├── violation_log/
│   └── violations.jsonl                # Attributed violation log (appended)
│
├── enforcer_report/
│   ├── report_data.json                # Machine-readable report
│   ├── report_<date>.txt               # Plain text report
│   └── report_<date>.pdf               # PDF report
│
├── outputs/
│   ├── week3/
│   │   ├── extractions.jsonl           # Clean data (60 records)
│   │   └── extractions_violated.jsonl  # Violated batch (confidence 0-100)
│   ├── traces/
│   │   └── runs.jsonl                  # LangSmith trace data (60 traces)
│   └── quarantine/                     # Failed prompt inputs land here
│
├── DEMO_SCRIPT.md            # Live demo script with commands + talking points
├── SUNDAY_REPORT_20260404.md # Weekly enforcement report (manual)
├── DOMAIN_NOTES.md           # Architecture and design decisions
└── requirements.txt
```

---

## Enforcement Modes

```
AUDIT   ──► log violations, never block. Use for new contracts in observation.
WARN    ──► exit 1 on CRITICAL severity only. Use for soft enforcement.
ENFORCE ──► exit 1 on CRITICAL or HIGH. Use for production pipelines in CI/CD.
```

In CI/CD, wrap the runner call:

```bash
py -3.13 -m contracts.runner \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json \
  --mode ENFORCE
if [ $? -ne 0 ]; then
  echo "Contract violation: blocking pipeline."
  exit 1
fi
```

---

## Key Design Decisions

**Why Bitol ODCS v3.0.0?**  
Open standard, YAML-native, covers schema, quality checks, lineage, and terms in one file. Tooling-agnostic — the enforcer reads the YAML directly, no intermediary registry.

**Why a separate contract registry?**  
The contract itself describes the producer's schema. The registry describes who consumes it and under what mode. These are separate concerns: a schema change is a producer decision; blast radius is a platform decision. Keeping them separate means the registry can evolve independently.

**Why local embeddings for drift detection?**  
`sentence-transformers/all-MiniLM-L6-v2` runs locally, requires no API key, and produces 384-dimensional embeddings suitable for centroid drift detection. External API calls for observability checks would create a circular dependency.

**Why is the confidence scale change a NARROW_TYPE?**  
A field going from `maximum: 1.0` to `maximum: 100.0` looks like a widening — but semantically it is a breaking narrowing because any consumer treating the value as a probability (0–1) will silently compute wrong results. The enforcer special-cases `is_confidence AND old_max ≤ 1.0 AND new_max ≥ 50` as `BREAKING / narrow_type`.

---

## Live Demo

See [DEMO_SCRIPT.md](DEMO_SCRIPT.md) for the full live demo script with exact commands, expected terminal output, and talking points for each of the six steps.
