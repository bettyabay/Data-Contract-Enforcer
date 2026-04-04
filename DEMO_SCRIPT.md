# Live Demo Script — Data Contract Enforcer
**Duration:** 6 min  |  **Python:** `py -3.13`  |  **cd to:** `Data-Contract-Enforcer/`

---

## Before You Record

```bash
# 1. Verify environment
py -3.13 -c "import pandas, yaml, sentence_transformers, reportlab, dotenv, anthropic; print('ENV OK')"

# 2. Verify clean state (each line must say OK)
[ ! -f violation_log/violations.jsonl ]         && echo "violations  OK" || echo "DIRTY"
[ ! -f validation_reports/week3_violated.json ] && echo "report      OK" || echo "DIRTY"
[ ! -f validation_reports/ai_extensions.json ]  && echo "ai_ext      OK" || echo "DIRTY"
[ ! -f enforcer_report/report_data.json ]       && echo "enf_report  OK" || echo "DIRTY"
```

If anything is DIRTY → run the **Reset** block at the bottom.

---

## Step 1 — Contract Generation `[0:00 – 1:00]`

**Say:** *"I'll generate a Bitol v3.0.0 data contract from 60 real extraction records.
The generator profiles every field, infers types and bounds, then calls Claude to annotate ambiguous columns like `extraction_model` and `source_path`."*

```bash
py -3.13 -m contracts.generator \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3_extractions \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/
```

**Look for:**
```
[generator] Loaded 60 records.
[generator] LLM annotating 9 ambiguous columns...
[generator] LLM annotations added for 5 column(s).
[generator] Contract written to generated_contracts\week3_extractions.yaml
[schema_analyzer] Snapshot written: schema_snapshots\week3_extractions\<timestamp>.json
```

**Open `generated_contracts/week3_extractions.yaml` and point to these 8 clauses:**

```yaml
doc_id:
  required: true          # (1) not-null
  format: uuid            # (2) format check
  pattern: ^[0-9a-f]...   # (3) regex pattern
  unique: true            # (4) uniqueness

extraction_model:
  enum: [claude-3-5-...]  # (5) allowed values only

fact_confidence:
  minimum: 0.0            # (6) range min  ← THIS FIRES IN STEP 2
  maximum: 1.0            # (7) range max  ← THIS FIRES IN STEP 2

quality:
  drift_checks:
  - type: statistical_drift   # (8) z-score drift detection
    field: fact_confidence
```

**Say:** *"Clause 6 and 7 — confidence must stay between 0.0 and 1.0. Now let's violate it."*

---

## Step 2 — Violation Detection `[1:00 – 2:00]`

**Say:** *"Same 60 records, but the extraction model started returning confidence as a percentage — 72.4 instead of 0.724.
Runner is in ENFORCE mode: CRITICAL violations exit code 1, which blocks CI/CD."*

```bash
py -3.13 -m contracts.runner \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json \
  --baselines validation_reports/week3_extractions_baseline.json \
  --mode ENFORCE
```

**Look for:**
```
[runner] Mode           : ENFORCE
[runner] Overall status : FAIL
[runner] Checks run     : 42  (passed=41, failed=1)

[FAIL|CRITICAL] range / extracted_facts[*].confidence:
  Data max (98.0) exceeds contract maximum (1.0).
  confidence is in 0-100 range, not 0.0-1.0. Breaking change detected.

[runner] ENFORCE mode: 1 blocking violation(s) detected. Exiting with code 1.
```

**Point out:**
- `FAIL | CRITICAL` — severity level
- `max=98.0` vs `contract max=1.0` — the exact mismatch
- `42 checks, 41 passed, 1 failed` — surgical: only the range clause fired
- `Exit code 1` — pipeline would stop here in CI

---

## Step 3 — Blame Chain + Blast Radius `[2:00 – 3:00]`

**Say:** *"The attributor does 4 things: registry lookup for downstream consumers,
BFS traversal of the Week 4 lineage graph to find the origin,
git blame on the upstream repo, then writes a structured violation log."*

```bash
py -3.13 -m contracts.attributor \
  --report validation_reports/week3_violated.json \
  --lineage "c:\Users\SNFD\Desktop\Tenacious Projects\Codebase-Understanding-Agent\outputs\week4\lineage_snapshots.jsonl" \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl
```

**Look for:**
```
[attributor] Attributed: [FAIL] extracted_facts[*].confidence / range
             -> registry_subscribers=2, lineage_nodes=0, top_candidate_score=0.01

[attributor] Wrote 1 attributed violation(s) to violation_log/violations.jsonl
```

**Then show the violation log:**

```bash
py -3.13 -c "
import json
v = json.loads(open('violation_log/violations.jsonl').readline())
print('BLAST RADIUS')
for s in v['blast_radius']['registry_subscribers']:
    print(f'  {s[\"subscriber_id\"]:25}  mode={s[\"validation_mode\"]}')
print()
print('BLAME CHAIN')
b = v['blame_chain'][0]
print(f'  author={b[\"author\"]}  confidence={b[\"confidence_score\"]}')
print(f'  {b[\"commit_message\"]}')
"
```

**Expected:**
```
BLAST RADIUS
  week4-cartographer         mode=ENFORCE
  week7-enforcer             mode=AUDIT

BLAME CHAIN
  author=unknown  confidence=0.01
  No git history found for field 'extracted_facts[*].confidence' upstream
```

**Say:** *"Two downstream systems in the blast radius. Week 4's Cartographer runs in ENFORCE —
this violation would block its ingestion pipeline.
Git blame found no history because this is synthetic data.
In production this traces back to the commit that changed the model's output format."*

---

## Step 4 — Schema Evolution `[3:00 – 4:00]`

**Say:** *"Now I'll diff two contract snapshots using the Bitol change taxonomy.
The clean snapshot from last week versus one taken after the breaking change.
Every field delta is classified as COMPATIBLE or BREAKING."*

```bash
py -3.13 -m contracts.schema_analyzer diff \
  --old schema_snapshots/week3_extractions/20260404_153046_16eb3cf9.json \
  --new schema_snapshots/week3_extractions/20260404_153500_broken.json \
  --output validation_reports/schema_evolution_week3.json
```

**Look for:**
```
[schema_analyzer] Overall  : BREAKING
[schema_analyzer] Changes  : 3  (BREAKING=2, COMPATIBLE=1)

  [compatible] fact_source_url: add_nullable
    New optional field — backward-compatible.

  [BREAKING] doc_id: remove_field
    Field 'doc_id' removed. Two-sprint minimum deprecation required.

  [BREAKING] fact_confidence: narrow_type
    Confidence scale changed 0.0-1.0 -> 0-100.0. CRITICAL: all downstream
    consumers using confidence as a probability will be silently corrupted.
    Statistical baselines must be deleted and re-established post-migration.

[schema_analyzer] Possible renames (1):
  doc_id -> fact_source_url  (confidence=0.8)
```

**Point out:**
- `narrow_type` — field stays `float`, no exception thrown, 100× wrong silently
- `remove_field` on `doc_id` — join key gone, cartographer loses document anchors
- rename detection at 0.8 confidence — it wasn't deleted, it was renamed

**Say:** *"The confidence scale change is the silent killer —
the type doesn't change, no error fires, every downstream probability calculation is just wrong.
The rename detector caught that doc_id was likely renamed to fact_source_url, not deleted."*

---

## Step 5 — AI Extensions `[4:00 – 5:15]`

**Say:** *"Standard contracts don't cover LLM behaviour — so we extended them.
Three checks: embedding drift on prompt inputs, JSON schema validation on every prompt,
and an output violation rate against the contract's allowed output schema.
Embeddings run locally — no API key needed."*

> **Heads up:** First run loads `all-MiniLM-L6-v2` (~15 s). Say: *"Loading the local embedding model..."*

```bash
py -3.13 -m contracts.ai_extensions \
  --traces outputs/traces/runs.jsonl \
  --contract generated_contracts/langsmith_traces.yaml \
  --baselines validation_reports/langsmith_traces_baseline.json \
  --output validation_reports/ai_extensions.json
```

**Look for:**
```
[ai_extensions] Overall status : FAIL
[ai_extensions] Checks run     : 3  (passed=1, warned=0, failed=2)

  [PASS] embedding_drift:
    Cosine distance from baseline: 0.0000  (threshold=0.15)

  [FAIL] prompt_input_schema:
    60 record(s) failed prompt input schema validation
    and were quarantined to outputs/quarantine.

  [FAIL] output_violation_rate:
    Output violation rate 100.00% exceeds FAIL threshold 5.00%.
    Sample issues: [{'issues': ['outputs.result is missing']}]
```

**Point out:**
- `embedding_drift PASS 0.0000` — prompt semantics unchanged; the model is getting the same kinds of queries
- `prompt_input_schema FAIL 60/60` — LangSmith stores `inputs: {}` at chain level; schema gap between platform and contract
- `output_violation_rate FAIL 100%` — `outputs.result` missing in every trace; the structured verdict field is absent

**Say:** *"These failures aren't model failures — they're data contract mismatches with
the observability platform. Without this check they'd be invisible."*

---

## Step 6 — Enforcer Report `[5:15 – 6:00]`

**Say:** *"Final step — the enforcer report. It aggregates all six contract runs,
the violation log, schema evolution, and AI checks into a health report.
Writes JSON, plain text, and a PDF."*

```bash
py -3.13 -m contracts.report_generator \
  --violations violation_log/violations.jsonl \
  --reports validation_reports/ \
  --ai-metrics validation_reports/ai_extensions.json \
  --evolution validation_reports/ \
  --output enforcer_report/
```

**Look for:**
```
[report_generator] Health score : 79.7/100
[report_generator] Summary      : 6 contracts, 339 checks, 1 failed, 1 violation attributed.
[report_generator] Recommended actions:
  1. [CRITICAL] Fix producer: output 'extracted_facts[*].confidence' within [0.0, 1.0].
  2. [HIGH] Run migration for week3_extractions: doc_id BREAKING change.
  3. [INFO] Schedule weekly runs to catch drift before it reaches downstream.
```

**Then show the health score from JSON:**

```bash
py -3.13 -c "
import json
r = json.load(open('enforcer_report/report_data.json'))
h, s = r['health_score'], r['summary']
print(f'Health  : {h[\"score\"]}/100  —  {h[\"narrative\"]}')
print(f'Checks  : {s[\"total_checks\"]} total  |  passed={s[\"passed\"]}  failed={s[\"failed\"]}')
print(f'Severity: CRITICAL={s[\"critical\"]}  HIGH={s[\"high\"]}  MEDIUM={s[\"medium\"]}')
"
```

**Then open the PDF:**

```bash
start enforcer_report/report_20260404.pdf
```

**Say:** *"79.7 out of 100. Six contracts, one critical issue.
This is the single artefact you'd present to a data platform team —
health score, violations, schema changes, AI risk, and three prioritised actions,
all auto-generated from the contract run."*

---

## Closing Line

> *"Six phases. One data contract.
> From raw JSONL to a generated SLA, a violation caught in CI,
> a blast radius mapped against a live lineage graph,
> a breaking schema change classified and costed,
> AI-specific drift and schema checks, and a PDF report —
> driven by a single Bitol YAML."*

---

## Reset Block

Run this to fully wipe demo outputs before recording:

```bash
cd "c:\Users\SNFD\Desktop\Tenacious Projects\Data-Contract-Enforcer"

rm -f violation_log/violations.jsonl
rm -f validation_reports/week3_violated.json
rm -f validation_reports/schema_evolution_week3.json
rm -f validation_reports/ai_extensions.json
rm -f validation_reports/migration_impact_*.json
rm -f enforcer_report/report_*.pdf enforcer_report/report_*.txt enforcer_report/report_data.json
rm -f outputs/quarantine/trace_quarantine.jsonl

echo "Reset complete. Run the pre-demo checklist above."
```

**Do NOT delete:** `*_baseline.json` files, `schema_snapshots/`, `outputs/week3/*.jsonl`
