# DOMAIN_NOTES.md — Data Contract Enforcer

Answers to the five required Phase 0 questions. All examples are drawn from real
outputs produced by this repo's sample-data generator and the canonical schemas
defined in the project document.

---

## Question 1 — What is a data contract and why does schema drift cause silent failures?

A **data contract** is a machine-checked promise between a data producer and a data
consumer. It specifies:

- **Structure**: which fields exist, their types, and whether they are required.
- **Semantics**: valid ranges, enum sets, formats (UUID, ISO 8601 date-time).
- **Statistics**: acceptable mean, standard deviation, and drift thresholds for
  numeric columns.
- **Lineage**: which upstream systems feed this dataset and which downstream systems
  consume it.

**Why silent failures occur:**

When a producer changes its output (e.g. the Document Refinery changes
`extracted_facts[].confidence` from a `0.0–1.0` float to a `0–100` integer
percentage) no Python `TypeError` is raised. The downstream consumer (e.g. the
Digital Courtroom rubric scorer) still reads a numeric value and computes a score —
but the score is now 100× too large. The pipeline runs. The outputs look plausible.
The bug is invisible until a human notices that every document is receiving
near-perfect rubric scores.

The Enforcer catches this by checking:

```
contract: extracted_facts.confidence → minimum: 0.0, maximum: 1.0
data:     max confidence observed = 99.0   → FAIL (range violation)
```

---

## Question 2 — Confidence scale change: the canonical silent-failure example

The project document describes a scenario where `extracted_facts[].confidence` is
changed from the range `0.0–1.0` to `0–100`. The Enforcer must detect this.

**Actual confidence distribution measured from `outputs/week3/extractions.jsonl`:**

```
min=0.650  max=0.990  mean=0.821  count=202
```

All values are within `[0.0, 1.0]` — the contract clause `minimum: 0.0,
maximum: 1.0` would PASS on this clean data.

**Injected violation** (see `outputs/week3/extractions_violated.jsonl`):

If `confidence` values are multiplied by 100 (simulating a producer bug):

```
min=65.0  max=99.0  mean=82.1
```

The ValidationRunner's range check would emit:

```
status: FAIL
check:  range
field:  fact_confidence
reason: data max (99.0) exceeds contract maximum (1.0)
```

**Bitol YAML clause for this field:**

```yaml
properties:
  fact_confidence:
    type: number
    minimum: 0.0
    maximum: 1.0
    description: >
      Confidence score for each extracted fact. MUST remain a 0.0-1.0 float.
      Changing this to a 0-100 percentage is a BREAKING schema change that
      silently corrupts every downstream rubric score.
    required: true
```

---

## Question 3 — Which inter-system interfaces have historically caused or could cause failures?

Based on the five-system data flow and the canonical schemas:

| Arrow | Risk | Reason |
|-------|------|--------|
| Week 3 → Week 2 | **HIGH** | `confidence` range ambiguity (0–1 vs 0–100); directly feeds rubric scoring |
| Week 3 → Week 8 (this system) | **HIGH** | `entity_refs[]` may reference IDs not in `entities[]` of same record |
| Week 5 → any consumer | **MEDIUM** | `sequence_number` monotonicity guarantee; gaps cause replay-state corruption |
| Week 4 → Week 8 | **MEDIUM** | Edge `source`/`target` must reference valid `node_id` in same snapshot |
| Week 1 → any consumer | **LOW** | `code_refs[]` non-empty constraint; empty list passes JSON parsing silently |

**Highest-priority contracts to generate first:** Week 3 extractions and Week 5 events,
because they have the most downstream consumers and the most numeric constraints.

---

## Question 4 — LangSmith trace contract (Bitol YAML snippet)

The LangSmith traces exported to `outputs/traces/runs.jsonl` carry the following
contract. This covers the AI extension checks described in Phase 2.

```yaml
# generated_contracts/langsmith-traces.yaml
apiVersion: v2.2.2
kind: DataContract
id: langsmith-traces
version: 1.0.0
description: >
  Contract for LangSmith agent traces exported to outputs/traces/runs.jsonl.
  Governs latency, token usage, error rate, and feedback score distribution.

schema:
  type: object
  properties:
    run_id:
      type: string
      format: uuid
      required: true
    run_type:
      type: string
      enum: [chain, llm, tool, retriever]
      required: true
    latency_ms:
      type: integer
      minimum: 0
      maximum: 120000
      description: >
        End-to-end latency in milliseconds. Breaching 120 000 ms indicates
        a hung LLM call and should trigger a WARN.
    total_tokens:
      type: integer
      minimum: 1
      description: Total tokens (prompt + completion). Zero is a data error.
    outputs.confidence:
      type: number
      minimum: 0.0
      maximum: 1.0
      description: >
        LLM-reported confidence from the outputs dict. Must remain 0.0-1.0.

quality:
  checks:
    - type: statistical_drift
      field: latency_ms
      z_score_warn: 2.0
      z_score_fail: 3.0
    - type: rate
      field: error
      name: error_rate
      maximum: 0.15
      description: >
        More than 15% of runs having a non-null error field signals a
        systemic LLM or infrastructure failure.

lineage:
  upstream:
    - id: "service::langsmith-api"
      description: "LangSmith hosted tracing service"
  downstream:
    - id: "file::contracts/ai_extensions.py"
      fields_consumed: [run_id, latency_ms, total_tokens, error, outputs]
```

---

## Question 5 — What deviations exist between actual outputs and canonical schemas?

The sample data in this repo was generated to match the canonical schemas. All
deviations from the canonical schemas are documented here and will be updated as
real prior-week outputs are integrated.

| System | Field | Canonical | Actual / Note |
|--------|-------|-----------|---------------|
| Week 3 | `extracted_facts[].page_ref` | `nullable int` | Present; ~10% of records have `null`. Contract sets `required: false`. |
| Week 5 | `metadata.causation_id` | `"uuid-v4 \| null"` | ~30% null (valid per spec). Contract sets `required: false`. |
| Week 4 | `edges[].confidence` | not in canonical | Added by cartographer; treated as informational, not enforced. |

**Migration scripts:** `outputs/migrate/` — currently empty; will be populated when
real prior-week data is integrated and deviations are found.
