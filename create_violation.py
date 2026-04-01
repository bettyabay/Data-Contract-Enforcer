"""
create_violation.py
Injects a confidence scale-change violation into week3 extractions.
Multiplies all extracted_facts[].confidence values by 100 (0.0–1.0 → 0–100).

Usage:
    python create_violation.py
Writes: outputs/week3/extractions_violated.jsonl
"""

import json
from pathlib import Path

SOURCE = "outputs/week3/extractions.jsonl"
OUTPUT = "outputs/week3/extractions_violated.jsonl"


def inject_scale_violation(source: str, output: str) -> int:
    records_modified = 0
    facts_modified = 0
    out_lines = []

    with open(source, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            for fact in record.get("extracted_facts", []):
                if "confidence" in fact:
                    fact["confidence"] = round(fact["confidence"] * 100, 2)
                    facts_modified += 1
            out_lines.append(json.dumps(record))
            records_modified += 1

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write("\n".join(out_lines) + "\n")

    print(f"[create_violation] Read  : {source}")
    print(f"[create_violation] Wrote : {output}")
    print(f"[create_violation] Records modified : {records_modified}")
    print(f"[create_violation] Facts modified   : {facts_modified}")
    print("[create_violation] Violation type   : confidence scale 0.0-1.0 -> 0-100")
    return records_modified


if __name__ == "__main__":
    inject_scale_violation(SOURCE, OUTPUT)
