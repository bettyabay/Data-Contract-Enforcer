"""
Generate realistic sample JSONL data for all 5 prior-week schemas.
Run once: python outputs/generate_sample_data.py
"""
import json
import random
import uuid
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

BASE_TIME = datetime(2025, 1, 15, 14, 0, 0, tzinfo=timezone.utc)


def ts(offset_hours=0):
    return (BASE_TIME + timedelta(hours=offset_hours, minutes=random.randint(0, 59))).isoformat().replace("+00:00", "Z")


def new_uuid():
    return str(uuid.uuid4())


def sha256_str(s):
    return hashlib.sha256(s.encode()).hexdigest()


# ── Week 1: Intent-Code Correlator ──────────────────────────────────────────

WEEK1_FILES = [
    "src/auth/login.py", "src/auth/middleware.py", "src/billing/invoice.py",
    "src/billing/payment.py", "src/document/refinery.py", "src/document/parser.py",
    "src/event/producer.py", "src/event/consumer.py", "src/lineage/graph.py",
    "src/lineage/cartographer.py", "src/api/routes.py", "src/utils/logger.py",
]
WEEK1_TAGS = ["auth", "pii", "billing", "document", "event", "lineage", "api"]
WEEK1_DESCRIPTIONS = [
    "Extract login credentials from request body and validate against user store",
    "Enforce JWT token expiry and refresh logic for protected routes",
    "Generate invoice PDF from line items and apply tax rules",
    "Process payment intent and update billing ledger atomically",
    "Extract structured facts from raw document text using LLM",
    "Parse markdown and PDF into normalised plain-text chunks",
    "Publish domain events to Kafka topic with schema validation",
    "Consume and replay events from dead-letter queue",
    "Build directed acyclic graph of data lineage from import analysis",
    "Snapshot codebase lineage and serialise to JSONL",
    "Route HTTP requests and apply rate-limiting middleware",
    "Structured log emission with correlation IDs",
]

week1_records = []
for i in range(15):
    file1 = random.choice(WEEK1_FILES)
    file2 = random.choice(WEEK1_FILES)
    line_start = random.randint(1, 200)
    line_end = line_start + random.randint(5, 60)
    week1_records.append({
        "intent_id": new_uuid(),
        "description": WEEK1_DESCRIPTIONS[i % len(WEEK1_DESCRIPTIONS)],
        "code_refs": [
            {
                "file": file1,
                "line_start": line_start,
                "line_end": line_end,
                "symbol": file1.split("/")[-1].replace(".py", "_handler"),
                "confidence": round(random.uniform(0.70, 0.99), 2),
            },
            {
                "file": file2,
                "line_start": random.randint(1, 100),
                "line_end": random.randint(101, 150),
                "symbol": file2.split("/")[-1].replace(".py", "_util"),
                "confidence": round(random.uniform(0.60, 0.95), 2),
            },
        ],
        "governance_tags": random.sample(WEEK1_TAGS, k=random.randint(1, 3)),
        "created_at": ts(i),
    })


# ── Week 2: Digital Courtroom ────────────────────────────────────────────────

CRITERIA = ["clarity", "correctness", "completeness", "relevance", "evidence_quality"]
VERDICTS = ["PASS", "FAIL", "WARN"]
RUBRIC_YAML = "rubric: {version: 1.2.0, criteria: [clarity, correctness, completeness]}"
RUBRIC_ID = sha256_str(RUBRIC_YAML)

week2_records = []
for i in range(20):
    scores = {}
    total_weight = 0
    weighted_sum = 0.0
    for crit in CRITERIA:
        s = random.randint(1, 5)
        scores[crit] = {
            "score": s,
            "evidence": [f"Sample evidence excerpt {random.randint(1,100)} for {crit}"],
            "notes": f"Evaluated {crit} based on content structure and factual grounding.",
        }
        weighted_sum += s
        total_weight += 1
    overall_score = round(weighted_sum / total_weight, 2)
    verdict = "PASS" if overall_score >= 3.5 else ("WARN" if overall_score >= 2.5 else "FAIL")
    week2_records.append({
        "verdict_id": new_uuid(),
        "target_ref": f"outputs/week3/doc_{i:03d}.pdf",
        "rubric_id": RUBRIC_ID,
        "rubric_version": "1.2.0",
        "scores": scores,
        "overall_verdict": verdict,
        "overall_score": overall_score,
        "confidence": round(random.uniform(0.75, 0.99), 2),
        "evaluated_at": ts(i * 2),
    })


# ── Week 3: Document Refinery ────────────────────────────────────────────────

ENTITY_TYPES = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]
MODELS = ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]

week3_records = []
for i in range(60):
    doc_id = new_uuid()
    source_path = f"outputs/source_docs/doc_{i:03d}.pdf"
    source_hash = sha256_str(source_path + str(i))
    n_entities = random.randint(2, 6)
    entities = [
        {
            "entity_id": new_uuid(),
            "name": random.choice(["Alice Johnson", "Acme Corp", "New York", "2025-01-10", "$4,500", "Protocol X"]),
            "type": random.choice(ENTITY_TYPES),
            "canonical_value": f"canonical_{j}",
        }
        for j in range(n_entities)
    ]
    entity_ids = [e["entity_id"] for e in entities]
    n_facts = random.randint(2, 5)
    facts = [
        {
            "fact_id": new_uuid(),
            "text": f"Extracted fact {k} from document {i}: relevant information about the subject matter.",
            "entity_refs": random.sample(entity_ids, k=min(random.randint(1, 2), len(entity_ids))),
            "confidence": round(random.uniform(0.65, 0.99), 2),
            "page_ref": random.randint(1, 20) if random.random() > 0.1 else None,
            "source_excerpt": f"Verbatim excerpt from page {random.randint(1,20)} of the source document.",
        }
        for k in range(n_facts)
    ]
    week3_records.append({
        "doc_id": doc_id,
        "source_path": source_path,
        "source_hash": source_hash,
        "extracted_facts": facts,
        "entities": entities,
        "extraction_model": random.choice(MODELS),
        "processing_time_ms": random.randint(800, 5000),
        "token_count": {
            "input": random.randint(2000, 8000),
            "output": random.randint(400, 1500),
        },
        "extracted_at": ts(i * 0.5),
    })


# ── Week 4: Brownfield Cartographer ─────────────────────────────────────────

NODE_TYPES = ["FILE", "TABLE", "SERVICE", "MODEL", "PIPELINE", "EXTERNAL"]
EDGE_RELATIONSHIPS = ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"]
LANGUAGES = ["python", "sql", "yaml", "javascript"]

def make_snapshot(snap_index):
    commit_sha = sha256_str(f"commit_{snap_index}")[:40]
    files = [
        "src/auth/login.py", "src/billing/invoice.py", "src/document/refinery.py",
        "src/event/producer.py", "src/lineage/graph.py", "src/api/routes.py",
        "outputs/week3/extractions.jsonl", "outputs/week5/events.jsonl",
    ]
    nodes = [
        {
            "node_id": f"file::{f}",
            "type": "FILE",
            "label": f.split("/")[-1],
            "metadata": {
                "path": f,
                "language": "python" if f.endswith(".py") else "json",
                "purpose": f"Handles {f.split('/')[-1].replace('.py','').replace('.jsonl','')} logic",
                "last_modified": ts(snap_index * 24),
            },
        }
        for f in files
    ]
    node_ids = [n["node_id"] for n in nodes]
    edges = [
        {
            "source": random.choice(node_ids),
            "target": random.choice(node_ids),
            "relationship": random.choice(EDGE_RELATIONSHIPS),
            "confidence": round(random.uniform(0.80, 0.99), 2),
        }
        for _ in range(random.randint(5, 12))
    ]
    # Ensure source != target
    edges = [e for e in edges if e["source"] != e["target"]]
    return {
        "snapshot_id": new_uuid(),
        "codebase_root": "/workspace/data-contract-enforcer",
        "git_commit": commit_sha,
        "nodes": nodes,
        "edges": edges,
        "captured_at": ts(snap_index * 24),
    }

week4_records = [make_snapshot(i) for i in range(3)]


# ── Week 5: Event Sourcing Platform ─────────────────────────────────────────

EVENT_TYPES = [
    "DocumentProcessed", "ContractGenerated", "ViolationDetected",
    "SchemaEvolved", "ReportPublished", "AgentInvoked",
]
AGGREGATE_TYPES = ["Document", "Contract", "Violation", "Schema", "Report", "Agent"]
SOURCE_SERVICES = [
    "week3-document-refinery", "week1-intent-correlator",
    "week4-brownfield-cartographer", "week2-digital-courtroom",
]

week5_records = []
aggregate_sequences = {}
for i in range(80):
    event_type = random.choice(EVENT_TYPES)
    agg_type = AGGREGATE_TYPES[EVENT_TYPES.index(event_type)]
    agg_id = new_uuid() if i < 10 else random.choice(
        [r["aggregate_id"] for r in week5_records[-10:]] if week5_records else [new_uuid()]
    )
    seq = aggregate_sequences.get(agg_id, 0) + 1
    aggregate_sequences[agg_id] = seq
    occurred = BASE_TIME + timedelta(hours=i * 0.3)
    recorded = occurred + timedelta(seconds=random.randint(1, 5))
    week5_records.append({
        "event_id": new_uuid(),
        "event_type": event_type,
        "aggregate_id": agg_id,
        "aggregate_type": agg_type,
        "sequence_number": seq,
        "payload": {"status": "ok", "item_count": random.randint(1, 100)},
        "metadata": {
            "causation_id": new_uuid() if random.random() > 0.3 else None,
            "correlation_id": new_uuid(),
            "user_id": f"user_{random.randint(1, 5)}",
            "source_service": random.choice(SOURCE_SERVICES),
        },
        "schema_version": "1.0",
        "occurred_at": occurred.isoformat().replace("+00:00", "Z"),
        "recorded_at": recorded.isoformat().replace("+00:00", "Z"),
    })


# ── LangSmith Traces ─────────────────────────────────────────────────────────

RUN_TYPES = ["chain", "llm", "tool", "retriever"]
trace_records = []
for i in range(60):
    start = BASE_TIME + timedelta(minutes=i * 5)
    latency = random.randint(400, 8000)
    end = start + timedelta(milliseconds=latency)
    trace_records.append({
        "run_id": new_uuid(),
        "trace_id": new_uuid(),
        "run_type": random.choice(RUN_TYPES),
        "name": random.choice(["extract_facts", "generate_contract", "validate_schema", "attribute_violation"]),
        "inputs": {"prompt": f"Sample prompt {i}", "context": "..."},
        "outputs": {"result": f"Sample output {i}", "confidence": round(random.uniform(0.6, 0.99), 2)},
        "error": None if random.random() > 0.1 else "TimeoutError: LLM response exceeded 30s",
        "start_time": start.isoformat().replace("+00:00", "Z"),
        "end_time": end.isoformat().replace("+00:00", "Z"),
        "latency_ms": latency,
        "total_tokens": random.randint(500, 6000),
        "prompt_tokens": random.randint(300, 4000),
        "completion_tokens": random.randint(100, 2000),
        "feedback": {
            "score": random.choice([0, 1]) if random.random() > 0.5 else None,
            "comment": None,
        },
        "tags": random.sample(["production", "dev", "eval", "week3", "week5"], k=2),
    })


# ── Write files ──────────────────────────────────────────────────────────────

outputs = {
    "outputs/week1/intent_records.jsonl": week1_records,
    "outputs/week2/verdicts.jsonl": week2_records,
    "outputs/week3/extractions.jsonl": week3_records,
    "outputs/week4/lineage_snapshots.jsonl": week4_records,
    "outputs/week5/events.jsonl": week5_records,
    "outputs/traces/runs.jsonl": trace_records,
}

for path, records in outputs.items():
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(records):>3} records -> {path}")

print("\nDone. All sample data generated.")
