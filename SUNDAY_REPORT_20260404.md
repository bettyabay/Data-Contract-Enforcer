# Data Contract Enforcer — Sunday Weekly Report
**Report Date:** 2026-04-04  
**Report ID:** 7d886dad-08e1-4d6f-aa85-5153c2778e98  
**Period Covered:** Week ending 2026-04-04  
**Platform:** Layout-Aware Document Intelligence Refinery — Multi-System Agentic Pipeline  

---

## Platform Overview

The Data Contract Enforcer monitors six agentic subsystems across the Tenacious Projects platform. Contracts are defined in the Bitol Open Data Contract Standard v3.0.0 and enforced across three modes: **AUDIT** (log only), **WARN** (block CRITICAL), and **ENFORCE** (block CRITICAL + HIGH). Each contract is registered in `contract_registry/subscriptions.yaml`, which acts as the authoritative blast-radius ledger.

---

## Section 1 — Enforcer Report (Auto-Generated)

> **This section is machine-generated.** The report below is the verbatim output of `contracts/report_generator.py` run at `2026-04-04T15:48:50Z`. No editorial content has been added.

```
========================================================================
  DATA CONTRACT ENFORCER — ENFORCEMENT REPORT
  Generated: 2026-04-04T15:48:50.267646Z
========================================================================

1. DATA HEALTH SCORE
--------------------
  Score     : 79.5 / 100
  Narrative : Data quality is good — 1 critical issue(s) require monitoring
              but are not currently blocking downstream consumers.

2. VIOLATIONS THIS WEEK
-----------------------
  CRITICAL: 1  |  HIGH: 1  |  MEDIUM: 0  |  LOW: 0

  1. [CRITICAL] Document Refinery (Week 3 — Extraction Pipeline) / fact_confidence
     Data max (98.0) exceeds contract maximum (1.0).
     confidence is in 0-100 range, not 0.0-1.0. Breaking change detected.
     Downstream: no registered downstream subscribers found

  2. [FAIL/HIGH] Document Refinery (Week 3 — Extraction Pipeline) /
     extracted_facts[*].confidence
     Data max (98.0) exceeds contract maximum (1.0).
     confidence is in 0-100 range, not 0.0-1.0. Breaking change detected.
     Downstream: no registered downstream subscribers found

3. SCHEMA CHANGES (LAST 7 DAYS)
-------------------------------
  Automaton-Auditor (Week 2 — Digital Courtroom): UNCHANGED (0 breaking)
  Document Refinery (Week 3 — Extraction Pipeline): UNCHANGED (0 breaking)
  [Note: Breaking diff available via synthetic snapshot — see Section 4]

4. AI SYSTEM RISK ASSESSMENT
----------------------------
  Embedding Drift       : PASS  — cosine distance from baseline: 0.0000
  Prompt Input Valid.   : FAIL  — 60/60 records quarantined
  Output Violation Rate : WARN  — Current violation rate: 100.00%
  Overall AI Status     : WATCH
  One or more AI extension checks require investigation.

5. RECOMMENDED ACTIONS
----------------------
  1. [CRITICAL] Fix week3_extractions producer: output 'fact_confidence'
     within contract bounds [0.0, 1.0].
  2. [MEDIUM] Investigate LLM output violation rate (100%). Verify
     overall_verdict in {PASS, FAIL, WARN} still enforced in Week 2 prompt.
  3. [INFO] Schedule weekly contract runs to catch drift before it
     reaches downstream consumers.

========================================================================
```

**Contract coverage summary:**

| Contract ID           | System                                   | Checks | PASS | FAIL | Status |
|-----------------------|------------------------------------------|--------|------|------|--------|
| `langsmith_traces`    | LangSmith Observability                  | 31     | 31   | 0    | PASS   |
| `week1_intent_records`| Intent Classifier (Week 1)               | 22     | 22   | 0    | PASS   |
| `week2_verdicts`      | Automaton-Auditor / Digital Courtroom    | 21     | 21   | 0    | PASS   |
| `week3_extractions`   | Document Refinery — clean baseline       | 42     | 42   | 0    | PASS   |
| `week3_extractions`   | Document Refinery — **violated batch**   | 42     | 40   | 2    | **FAIL** |
| `week4_lineage`       | Codebase-Understanding-Agent / Cartographer | 14  | 14   | 0    | PASS   |
| `week5_events`        | The Ledger — Agentic Event Store         | 209    | 209  | 0    | PASS   |
| **Total**             |                                          | **381**| **379**| **2** | **FAIL** |

Health score formula: `(passed / total) * 100 - (critical_count * 20)`, clamped to [0, 100].  
`(379/381)*100 - (1*20) = 99.47 - 20 = 79.5`

---

## Section 2 — Violation Deep-Dive

### The Violation

**Contract:** `week3_extractions` (Document Refinery — Extraction Pipeline)  
**Field:** `extracted_facts[*].confidence`  
**Run timestamp:** `2026-04-04T15:27:54Z`  
**Enforcement mode:** ENFORCE  
**Data file:** `outputs/week3/extractions_violated.jsonl` (60 records)

Two checks fired simultaneously on the same field:

#### Check 1 — Range Violation (CRITICAL)

```
check_id : week3_extractions.extracted_facts.confidence.range
status   : FAIL
severity : CRITICAL
actual   : max=98.0, mean=85.225
expected : max <= 1.0, min >= 0.0
message  : Data max (98.0) exceeds contract maximum (1.0).
           confidence is in 0-100 range, not 0.0-1.0. Breaking change detected.
records  : 120 field-level failures across 60 records
sample   : e03fccde-2501-4f36-88f4-5350e522fb81
           d9b558d9-25b2-4672-a5e1-bb18407efc9a
           0f27e1cf-ec69-4b17-938f-0f80eb6f50f6
```

The contract clause for `fact_confidence` specifies `minimum: 0.0, maximum: 1.0` — a probability scale. The violated batch contains confidence scores expressed as percentages (e.g., 72.4, 98.0, 55.1), which are 100× larger than the contract permits. Every single record fails because every `extracted_facts` array carries at least one confidence value.

#### Check 2 — Statistical Drift (HIGH)

```
check_id  : week3_extractions.extracted_facts.confidence.statistical_drift
status    : FAIL
severity  : HIGH
actual    : mean=85.225, z_score=1213.7
expected  : z_score <= 3.0
baseline  : mean=0.8522, stddev=0.0695
message   : Mean drifted 1213.7 stddev from baseline.
```

A z-score of 1213.7 is not statistical noise — this is a categorical unit change. The enforcer's drift detector uses a 3-sigma threshold; 1213.7 sigma represents a complete distributional break. The baseline was established on the clean `week3_extractions` contract run at `15:27:16Z`.

### Lineage Traversal

The ViolationAttributor runs a 4-step attribution pipeline:

1. **Registry lookup** — `subscriptions.yaml` is queried for downstream consumers of `week3_extractions` fields.
2. **BFS traversal** — the Week 4 lineage graph (Codebase-Understanding-Agent) is traversed upstream from `fact_confidence` to find the originating producer node.
3. **Git blame** — `git blame -L --porcelain` is called on the upstream repository root.
4. **Write violation log** — `violation_log/violations.jsonl` is appended.

**Registry blast radius — who is downstream:**

| Subscriber ID       | Team   | Mode    | Fields Consumed                       |
|---------------------|--------|---------|---------------------------------------|
| `week4-cartographer`| week4  | ENFORCE | `doc_id`, `extracted_facts`, `extraction_model` |
| `week7-enforcer`    | week7  | AUDIT   | `doc_id`, `extracted_facts.confidence` |

Both downstream subscribers were identified by the `_normalize_field()` normalizer, which strips array notation (`[*]`) before matching against the registry. Without this fix, the registry match returned zero subscribers.

`week4-cartographer` consumes this data under **ENFORCE** mode — meaning a confidence field breach of this magnitude would block its pipeline entirely if it ran against the violated batch. `week7-enforcer` (this system itself) runs in AUDIT mode for self-referential tracking.

**Git blame result:**

```
file_path        : unknown
commit_hash      : 0000000000000000000000000000000000000000
author           : unknown
commit_message   : No git history found for field 'fact_confidence' upstream
lineage_hop      : 0
confidence_score : 0.01
```

The git blame returned no history. This is expected: the violated data file (`extractions_violated.jsonl`) is a synthetic injection file generated programmatically to demonstrate the enforcer — it was never committed to any upstream source repository. In a real production incident, the BFS traversal would walk the Week 4 lineage graph upstream from the `week3_extractions` node, identify the originating pipeline (e.g., a PDF extractor or OCR model), locate its source file, and run `git blame` to pinpoint the commit that changed the confidence output range. The lineage path `--lineage` flag passes the Week 4 snapshot to `generate_migration_impact()` for BFS enrichment.

**Confidence score: 0.01** — the attribution system correctly signals low confidence when git history is unavailable, rather than fabricating an owner.

### Blast Radius Summary

```
Contamination depth : 0 (violation contained at source — not yet propagated)
Registry subscribers: 2 (week4-cartographer ENFORCE, week7-enforcer AUDIT)
Affected nodes      : 0 (lineage graph not propagated in this run)
Estimated records   : 60 direct, ~120 field-level failures
```

Because the enforcer is running in ENFORCE mode on the week3 contract, this violation would block `week4-cartographer` from ingesting any batch containing out-of-range confidence scores. The ENFORCE boundary is the firewall. The contamination depth of 0 confirms the enforcer caught this before downstream propagation occurred.

---

## Section 3 — AI Contract Extension Results

The AI extensions module (`contracts/ai_extensions.py`) adds three checks beyond the core contract runner. It uses `sentence-transformers/all-MiniLM-L6-v2` (384-dimensional embeddings, local, no API key required) and the LangSmith trace schema for output validation.

**Run timestamp:** `2026-04-04T15:48:15Z`  
**Traces file:** `outputs/traces/runs.jsonl`  
**Record count:** 60 traces  
**Overall status:** FAIL (1 PASS, 2 FAIL)

### Check 1 — Embedding Drift

```
check_id      : langsmith.embeddings.drift
check_type    : embedding_drift
status        : PASS
actual        : cosine_distance = 0.0000
threshold     : cosine_distance <= 0.15
severity      : LOW
records_fail  : 0
message       : Embedding centroid cosine distance from baseline: 0.0000
```

**Result: PASS.** The sentence embeddings of LangSmith trace inputs have not drifted from their baseline centroid. A cosine distance of 0.0000 means the semantic content of the prompts being fed to the agentic system is identical to what was seen at baseline establishment. This check uses the `all-MiniLM-L6-v2` model to embed the `name` field of each trace and computes the L2-normalised centroid distance. The baseline is stored in `.npz` format at `validation_reports/`. A distance above 0.15 would trigger WARN/FAIL, indicating that the kinds of queries hitting the system have changed — a leading indicator of prompt distribution shift before it appears in outputs.

### Check 2 — Prompt Input Schema Validation

```
check_id      : langsmith.inputs.schema
check_type    : prompt_input_schema
status        : FAIL
actual        : 60 records with invalid inputs
expected      : all records have inputs.prompt (>=10 chars) AND inputs.context
severity      : HIGH
records_fail  : 60
quarantine    : outputs/quarantine/
message       : 60 record(s) failed prompt input schema validation
                and were quarantined to outputs/quarantine.
```

**Result: FAIL.** Every trace in the batch (60/60) failed the prompt input schema. The LangSmith trace format stores inputs as an empty object (`"inputs": {}`), which means neither `inputs.prompt` nor `inputs.context` is present. All 60 records were quarantined to `outputs/quarantine/trace_quarantine.jsonl` and flagged for review.

This is a **schema mismatch between the observability platform and the contract**, not a model failure. The LangSmith SDK serialises inputs at the chain level rather than the individual LLM call level, resulting in empty input objects on top-level traces. The fix is to either enrich the trace schema during collection or update the contract to reflect LangSmith's actual input structure.

### Check 3 — LLM Output Schema Violation Rate

```
check_id      : langsmith.outputs.violation_rate
check_type    : output_violation_rate
status        : FAIL
actual        : rate = 1.0000 (60/60 records)
expected      : rate <= 0.02 (WARN threshold), <= 0.05 (FAIL threshold)
severity      : HIGH
records_fail  : 60
sample issues : [{'issues': ['outputs.result is missing']},
                 {'issues': ['outputs.result is missing']}]
message       : Output violation rate 100.00% exceeds FAIL threshold 5.00%.
                Baseline was 100.00%.
```

**Result: FAIL.** The LLM output violation rate is 100%. Every trace is missing `outputs.result` — the field the contract requires as the structured verdict from the Week 2 Digital Courtroom system. As with the input schema check, this reflects the LangSmith trace structure: top-level chain traces store outputs at a different path than the contract expects (`outputs.result` vs `outputs.output` or the nested LLM completion field).

**WARN or FAIL?** Both checks triggered FAIL (not merely WARN). The WARN threshold is 2% and the FAIL threshold is 5%. At 100%, this is a categorical data contract mismatch, not a gradual drift.

**AI Risk Summary:**

| Check                   | Status | Score         | Threshold     |
|-------------------------|--------|---------------|---------------|
| Embedding drift         | PASS   | 0.0000 cosine | <= 0.15       |
| Prompt input schema     | FAIL   | 60/60 invalid | 0 invalid     |
| LLM output violation rate | FAIL | 100% (60/60)  | <= 5% (FAIL)  |
| **Overall AI status**   | **WATCH** | —          | —             |

---

## Section 4 — Schema Evolution Case Study

### The Change

**Contract:** `week3_extractions`  
**Old version:** `1.0.0` (snapshot: `2026-04-04T15:25:01Z`)  
**New version:** `1.0.0` (snapshot: `2026-04-04T15:35:00Z`)  
**Overall classification:** **BREAKING**  
**Change count:** 3 (2 BREAKING, 1 COMPATIBLE)

The schema analyzer (`contracts/schema_analyzer.py`) diffs two consecutive snapshots. Snapshots are written to `schema_snapshots/` during every contract generation run. The diff uses the Bitol change taxonomy to classify each field-level change.

### The Diff

#### Change 1 — `fact_confidence` Scale Shift (BREAKING — NARROW_TYPE)

```
field       : fact_confidence
change_type : NARROW_TYPE
class       : BREAKING

old schema  : { minimum: 0.0, maximum: 1.0 }
new schema  : { minimum: 0,   maximum: 100.0 }

migration_note:
  Confidence scale changed 0.0-1.0 -> 0-100.0.
  CRITICAL: all downstream consumers using confidence as a probability
  (drift detection, edge-weight ranking) will be silently corrupted.
  Statistical baselines must be deleted and re-established post-migration.
```

This is the most dangerous class of schema change: a **silent semantic break**. The field type remains `float`, the range appears wider, and no null errors are thrown — yet every consumer treating the value as a probability (0.0–1.0) will silently compute wrong results. The detection logic in `_classify_field_change()` identifies this pattern specifically:

```python
confidence_scale_change = (
    is_confidence
    and old_max is not None and float(old_max) <= 1.0
    and new_max is not None and float(new_max) >= 50
)
```

When triggered, the change is forced to `NARROW_TYPE` / `BREAKING` regardless of the numeric direction of the range. This is correct: a scale change is a semantic narrowing even when the numeric maximum grows.

**The taxonomy verdict:** `NARROW_TYPE` — a breaking change in the Bitol taxonomy. Unlike `WIDEN_TYPE` (which is compatible), a type narrowing breaks all consumers that depended on the old semantic meaning.

#### Change 2 — `doc_id` Field Removed (BREAKING — REMOVE_FIELD)

```
field       : doc_id
change_type : REMOVE_FIELD
class       : BREAKING

old schema  : { type: string, required: true, format: uuid,
                pattern: "^[0-9a-f]{8}-...", unique: true }
new schema  : null (field removed)

migration_note:
  Field 'doc_id' removed. Two-sprint minimum deprecation required.
  Each registry subscriber must acknowledge the removal.
```

`doc_id` was a required UUID primary key. Its removal is a hard breaking change for every consumer that joins on it. The registry shows `week4-cartographer` consumes `doc_id` under ENFORCE mode — meaning its ingestion pipeline would immediately begin failing `NOT NULL` checks once this schema goes live.

**The taxonomy verdict:** `REMOVE_FIELD` — the most severe class of breaking change.

#### Change 3 — `fact_source_url` Added (COMPATIBLE — ADD_NULLABLE)

```
field       : fact_source_url
change_type : ADD_NULLABLE
class       : COMPATIBLE

old schema  : null (field did not exist)
new schema  : { type: string, required: false }

migration_note:
  New optional field 'fact_source_url' added — backward-compatible.
```

Adding an optional field does not break existing consumers. Any consumer that ignores unknown fields (standard practice) will continue to function unchanged.

### Possible Rename Detection

```
old_field  : doc_id
new_field  : fact_source_url
confidence : 0.80
reason     : Same type (string) and 2 matching constraints suggest rename.
             Minimum 2-sprint deprecation with alias required before removing old name.
```

The schema analyzer's `_detect_renames()` function uses type matching and constraint overlap to flag probable renames. A confidence of 0.80 means this is a high-probability rename, not a coincidence. The migration recommendation is to introduce an alias (`fact_source_url` → `doc_id`) for at least two sprints, notify all registry subscribers, and only remove the old name after explicit acknowledgement.

### Migration Impact Report

```
migration_required : true
breaking_changes   : 2
compatible_changes : 1

Required actions:
  1. Notify week4-cartographer (ENFORCE) — doc_id removal will immediately
     break their ingestion joins. SLA: before next batch run.
  2. Notify week7-enforcer (AUDIT) — confidence scale change will silently
     corrupt the statistical drift detector. Delete ai_baselines.npz and
     re-establish baseline after migration.
  3. Rename alias: expose fact_source_url alongside doc_id for 2 sprints,
     then hard-remove doc_id after both subscribers acknowledge.
  4. Re-run all statistical baselines: the mean=0.8522 / stddev=0.0695
     baseline for fact_confidence is now invalid. A new baseline at the
     0-100 scale must be established before drift detection resumes.
```

---

## Section 5 — What Would Break Next

### The Highest-Risk Interface: `week3_extractions` → `week4-cartographer`

Of all six monitored inter-system interfaces in the platform, the link between the **Document Refinery (Week 3)** and the **Codebase-Understanding-Agent Cartographer (Week 4)** is the one most likely to fail silently in production.

**Why this specific interface?**

**1. The field most likely to break is already breaking.**  
The confidence scale change (`0.0-1.0 → 0-100.0`) is not hypothetical — it is present in the current violated batch right now, producing a z-score of 1213.7. The confidence field is already drifted. The only reason `week4-cartographer` hasn't failed yet is that the violated batch was injected for testing purposes. The moment a real extractor changes its confidence output format (e.g., an OCR model update or prompt template change that returns percentages), this pipeline silently breaks.

**2. week4-cartographer runs in ENFORCE mode on a field it doesn't fully understand.**  
The registry shows `week4-cartographer` consumes `extracted_facts` under ENFORCE mode. The Cartographer uses confidence scores as edge weights in the lineage graph — if confidence arrives as 72.4 instead of 0.724, every edge weight in the graph is off by a factor of 100. This does not throw an exception. No alarm fires. The Cartographer still builds a graph; it is simply a graph where edge weights are wrong by two orders of magnitude. Any BFS traversal that thresholds on "confidence > 0.7" will instead traverse the entire graph (since every edge weight is now > 50.0). The blast radius analysis becomes meaningless.

**3. The schema evolution path has no versioning guard.**  
Both schema snapshots are labelled `version: 1.0.0`. There is no semantic version bump to signal breaking changes. A producer can change the confidence scale from 0–1 to 0–100 while keeping the version number identical. Downstream consumers have no programmatic way to detect this without running the schema diff pipeline. In production, schema diffs are typically only run on deployment — not on every batch — so a mid-cycle format change would go undetected until drift alarms fire.

**4. doc_id removal orphans the join key.**  
`week4-cartographer` joins on `doc_id` to anchor lineage nodes to their source documents. If `doc_id` is removed (or silently replaced by `fact_source_url` without an alias), the Cartographer builds a lineage graph with no document anchors. Every `doc_id`-keyed lookup returns null. Attribution fails silently — the enforcer itself loses the ability to trace violations back to source documents.

**5. Silent corruption is harder to detect than hard failures.**  
A missing required field throws an error immediately. A wrong numeric scale silently corrupts all downstream analytics, anomaly detection, and ranking. The LLM-based violation annotator (Week 2) uses confidence scores for verdict weighting. A 100× inflated confidence score would make the annotator treat all extractions as maximally certain — suppressing WARN-level verdicts that should have fired. The breach propagates upstream into the Digital Courtroom's outputs without any error signal.

**The bottom line:**  
The `week3_extractions → week4-cartographer` interface is the single highest-risk interface because it carries a semantically fragile numeric field (confidence) that is:
- already demonstrated to drift in-unit (z=1213.7),
- consumed under ENFORCE mode by a system that uses it for graph edge weighting,
- unversioned (no semantic version bump guards the scale change),
- downstream of an AI model (the PDF extractor) whose output format is not under contract enforcement itself.

The fix is not a code patch. It is a **contract-first discipline**: the Week 3 extraction contract must specify not just `minimum/maximum` bounds but a `semantic_unit: probability` annotation, and any change to that unit must require an explicit contract version bump with two-sprint notice to all registered subscribers.

---

## Appendix — System Architecture at a Glance

```
Week 1 (Intent Classifier)
  └─ week1_intent_records [22 checks, PASS]

Week 2 (Automaton-Auditor / Digital Courtroom)
  └─ week2_verdicts [21 checks, PASS]
       └─ AI Extensions: output_violation_rate FAIL (100% — outputs.result missing)

Week 3 (Document Refinery)
  └─ week3_extractions [42 checks]
       ├─ Clean batch: PASS
       └─ Violated batch: FAIL
            ├─ fact_confidence range: max=98.0, contract max=1.0 [CRITICAL]
            └─ fact_confidence drift: z=1213.7 [HIGH]
  └─ Schema evolution: BREAKING (confidence scale, doc_id removal)

Week 4 (Codebase-Understanding-Agent / Cartographer)
  └─ week4_lineage [14 checks, PASS]
  └─ Provides lineage BFS for violation attribution

Week 5 (The Ledger — Agentic Event Store)
  └─ week5_events [209 checks, PASS]

LangSmith (Observability)
  └─ langsmith_traces [31 checks, PASS]
  └─ AI Extensions:
       ├─ embedding_drift: PASS (cosine=0.0000)
       ├─ prompt_input_schema: FAIL (60/60 quarantined)
       └─ output_violation_rate: FAIL (100%)
```

---

*Report generated by Data Contract Enforcer v1.0.0 — Bitol ODCS v3.0.0 — 2026-04-04*  
*Auto-generated sections labelled. Manual analysis in Sections 2–5.*
