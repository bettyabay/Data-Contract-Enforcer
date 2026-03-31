"""Tests for contracts/generator.py"""
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from contracts.generator import (
    build_contract,
    column_to_clause,
    flatten_for_profile,
    generate,
    infer_type,
    load_jsonl,
    profile_column,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

MINIMAL_RECORDS = [
    {
        "doc_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "extracted_facts": [
            {"fact_id": "f1f1f1f1-0000-0000-0000-000000000001", "confidence": 0.9, "page_ref": 1},
            {"fact_id": "f1f1f1f1-0000-0000-0000-000000000002", "confidence": 0.7, "page_ref": None},
        ],
        "processing_time_ms": 1200,
        "extracted_at": "2025-01-15T14:23:00Z",
    },
    {
        "doc_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
        "extracted_facts": [
            {"fact_id": "f2f2f2f2-0000-0000-0000-000000000003", "confidence": 0.85, "page_ref": 3},
        ],
        "processing_time_ms": 800,
        "extracted_at": "2025-01-15T15:00:00Z",
    },
]

VIOLATED_RECORDS = [
    {
        "doc_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "extracted_facts": [
            {"fact_id": "f3f3f3f3-0000-0000-0000-000000000001", "confidence": 90.0, "page_ref": 1},
        ],
        "processing_time_ms": 1200,
        "extracted_at": "2025-01-15T14:23:00Z",
    },
]


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def minimal_jsonl(tmp_dir):
    path = tmp_dir / "test_data.jsonl"
    with open(path, "w") as f:
        for r in MINIMAL_RECORDS:
            f.write(json.dumps(r) + "\n")
    return str(path)


@pytest.fixture
def violated_jsonl(tmp_dir):
    path = tmp_dir / "violated_data.jsonl"
    with open(path, "w") as f:
        for r in VIOLATED_RECORDS:
            f.write(json.dumps(r) + "\n")
    return str(path)


# ── load_jsonl ────────────────────────────────────────────────────────────────

def test_load_jsonl_returns_list(minimal_jsonl):
    records = load_jsonl(minimal_jsonl)
    assert isinstance(records, list)
    assert len(records) == 2


def test_load_jsonl_skips_blank_lines(tmp_path):
    p = tmp_path / "sparse.jsonl"
    p.write_text('{"a": 1}\n\n{"b": 2}\n\n')
    records = load_jsonl(str(p))
    assert len(records) == 2


def test_load_jsonl_preserves_nested_fields(minimal_jsonl):
    records = load_jsonl(minimal_jsonl)
    assert "extracted_facts" in records[0]
    assert isinstance(records[0]["extracted_facts"], list)


# ── flatten_for_profile ───────────────────────────────────────────────────────

def test_flatten_explodes_array_field():
    records = MINIMAL_RECORDS
    df = flatten_for_profile(records)
    # 2 facts in record 0 + 1 fact in record 1 = 3 rows
    assert len(df) == 3


def test_flatten_prefixes_array_columns():
    df = flatten_for_profile(MINIMAL_RECORDS)
    assert "fact_confidence" in df.columns


def test_flatten_preserves_scalar_columns():
    df = flatten_for_profile(MINIMAL_RECORDS)
    assert "doc_id" in df.columns
    assert "processing_time_ms" in df.columns


def test_flatten_no_arrays():
    records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    df = flatten_for_profile(records)
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2


# ── profile_column ────────────────────────────────────────────────────────────

def test_profile_numeric_column_has_stats():
    s = pd.Series([0.7, 0.8, 0.9, 0.85, 0.95])
    profile = profile_column(s, "confidence")
    assert "stats" in profile
    assert profile["stats"]["min"] == pytest.approx(0.7)
    assert profile["stats"]["max"] == pytest.approx(0.95)


def test_profile_null_fraction():
    s = pd.Series([1.0, None, 2.0, None])
    profile = profile_column(s, "value")
    assert profile["null_fraction"] == pytest.approx(0.5)


def test_profile_string_column_no_stats():
    s = pd.Series(["a", "b", "c"])
    profile = profile_column(s, "label")
    assert "stats" not in profile


# ── infer_type ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dtype,expected", [
    ("float64", "number"),
    ("int64", "integer"),
    ("bool", "boolean"),
    ("object", "string"),
    ("unknown_dtype", "string"),
])
def test_infer_type(dtype, expected):
    assert infer_type(dtype) == expected


# ── column_to_clause ──────────────────────────────────────────────────────────

def test_clause_confidence_gets_range():
    profile = {
        "name": "fact_confidence",
        "dtype": "float64",
        "null_fraction": 0.0,
        "cardinality_estimate": 100,
        "sample_values": ["0.7", "0.9"],
        "stats": {"min": 0.7, "max": 0.9, "mean": 0.8, "p25": 0.75, "p50": 0.8,
                  "p75": 0.85, "p95": 0.89, "p99": 0.9, "stddev": 0.07},
    }
    clause = column_to_clause(profile)
    assert clause["minimum"] == 0.0
    assert clause["maximum"] == 1.0
    assert clause["type"] == "number"


def test_clause_id_field_gets_uuid_format():
    profile = {
        "name": "doc_id",
        "dtype": "object",
        "null_fraction": 0.0,
        "cardinality_estimate": 50,
        "sample_values": ["a1b2c3d4-e5f6-7890-abcd-ef1234567890"],
    }
    clause = column_to_clause(profile)
    assert clause["format"] == "uuid"


def test_clause_at_field_gets_datetime_format():
    profile = {
        "name": "extracted_at",
        "dtype": "object",
        "null_fraction": 0.0,
        "cardinality_estimate": 10,
        "sample_values": ["2025-01-15T14:23:00Z"],
    }
    clause = column_to_clause(profile)
    assert clause["format"] == "date-time"


def test_clause_required_false_when_nulls():
    profile = {
        "name": "page_ref",
        "dtype": "float64",
        "null_fraction": 0.1,
        "cardinality_estimate": 20,
        "sample_values": ["1", "2"],
        "stats": {"min": 1.0, "max": 20.0, "mean": 5.0, "p25": 2.0, "p50": 5.0,
                  "p75": 8.0, "p95": 18.0, "p99": 20.0, "stddev": 3.0},
    }
    clause = column_to_clause(profile)
    assert clause["required"] is False


# ── generate (integration) ────────────────────────────────────────────────────

def test_generate_creates_yaml_file(minimal_jsonl, tmp_dir):
    out_dir = str(tmp_dir / "contracts")
    result_path = generate(
        source=minimal_jsonl,
        contract_id="test-contract",
        lineage=None,
        output_dir=out_dir,
    )
    assert result_path.exists()
    with open(result_path) as f:
        contract = yaml.safe_load(f)
    assert contract["id"] == "test-contract"
    assert contract["kind"] == "DataContract"


def test_generate_includes_confidence_bounds(minimal_jsonl, tmp_dir):
    out_dir = str(tmp_dir / "contracts")
    result_path = generate(
        source=minimal_jsonl,
        contract_id="test-contract",
        lineage=None,
        output_dir=out_dir,
    )
    with open(result_path) as f:
        contract = yaml.safe_load(f)
    props = contract["schema"]["properties"]
    assert "fact_confidence" in props
    assert props["fact_confidence"]["minimum"] == 0.0
    assert props["fact_confidence"]["maximum"] == 1.0


def test_generate_record_count_matches(minimal_jsonl, tmp_dir):
    out_dir = str(tmp_dir / "contracts")
    result_path = generate(
        source=minimal_jsonl,
        contract_id="test-contract",
        lineage=None,
        output_dir=out_dir,
    )
    with open(result_path) as f:
        contract = yaml.safe_load(f)
    assert contract["source"]["record_count"] == len(MINIMAL_RECORDS)
