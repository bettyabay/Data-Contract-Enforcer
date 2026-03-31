"""Tests for contracts/runner.py"""
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from contracts.runner import (
    check_datetime_format,
    check_enum,
    check_ranges,
    check_required,
    check_statistical_drift,
    check_types,
    check_uuid_pattern,
    run_validation,
)
from contracts.generator import generate


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_df(**kwargs):
    """Build a DataFrame from keyword column=list pairs."""
    return pd.DataFrame(kwargs)


def make_props(**kwargs):
    """Build a properties dict from keyword args."""
    return kwargs


# ── check_required ────────────────────────────────────────────────────────────

def test_required_passes_when_no_nulls():
    df = make_df(name=["Alice", "Bob"])
    props = make_props(name={"type": "string", "required": True})
    violations = []
    check_required(df, props, violations)
    assert violations == []


def test_required_fails_when_nulls_present():
    df = make_df(name=["Alice", None])
    props = make_props(name={"type": "string", "required": True})
    violations = []
    check_required(df, props, violations)
    assert len(violations) == 1
    assert violations[0]["status"] == "CRITICAL"
    assert violations[0]["check"] == "required_field_present"


def test_required_fails_when_field_absent():
    df = make_df(other=["x"])
    props = make_props(name={"type": "string", "required": True})
    violations = []
    check_required(df, props, violations)
    assert violations[0]["status"] == "CRITICAL"


# ── check_types ───────────────────────────────────────────────────────────────

def test_type_check_passes_numeric():
    df = make_df(score=[0.9, 0.7, 0.8])
    props = make_props(score={"type": "number"})
    violations = []
    check_types(df, props, violations)
    assert violations == []


def test_type_check_fails_string_for_number():
    df = make_df(score=["high", "low", "medium"])
    props = make_props(score={"type": "number"})
    violations = []
    check_types(df, props, violations)
    assert violations[0]["status"] == "CRITICAL"
    assert violations[0]["check"] == "type_match"


# ── check_enum ────────────────────────────────────────────────────────────────

def test_enum_passes_all_valid():
    df = make_df(verdict=["PASS", "FAIL", "PASS"])
    props = make_props(verdict={"type": "string", "enum": ["PASS", "FAIL", "WARN"]})
    violations = []
    check_enum(df, props, violations)
    assert violations == []


def test_enum_fails_invalid_value():
    df = make_df(verdict=["PASS", "INVALID", "FAIL"])
    props = make_props(verdict={"type": "string", "enum": ["PASS", "FAIL", "WARN"]})
    violations = []
    check_enum(df, props, violations)
    assert len(violations) == 1
    assert violations[0]["status"] == "FAIL"
    assert "INVALID" in str(violations[0]["sample"])


# ── check_ranges ──────────────────────────────────────────────────────────────

def test_range_passes_within_bounds():
    df = make_df(confidence=[0.7, 0.8, 0.9])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    violations = []
    check_ranges(df, props, violations)
    assert violations == []


def test_range_fails_above_maximum():
    df = make_df(confidence=[70.0, 80.0, 90.0])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    violations = []
    check_ranges(df, props, violations)
    assert len(violations) == 1
    assert violations[0]["status"] == "CRITICAL"
    assert "range" == violations[0]["check"]


def test_range_fails_below_minimum():
    df = make_df(score=[-5.0, 2.0, 3.0])
    props = make_props(score={"type": "number", "minimum": 0.0, "maximum": 10.0})
    violations = []
    check_ranges(df, props, violations)
    assert violations[0]["status"] == "CRITICAL"


# ── check_uuid_pattern ────────────────────────────────────────────────────────

def test_uuid_passes_valid_uuids():
    df = make_df(doc_id=[
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "b2c3d4e5-f6a7-8901-bcde-f12345678901",
    ])
    props = make_props(doc_id={"type": "string", "format": "uuid"})
    violations = []
    check_uuid_pattern(df, props, violations)
    assert violations == []


def test_uuid_fails_invalid_value():
    df = make_df(doc_id=["not-a-uuid", "also-not-valid"])
    props = make_props(doc_id={"type": "string", "format": "uuid"})
    violations = []
    check_uuid_pattern(df, props, violations)
    assert len(violations) == 1
    assert violations[0]["status"] == "FAIL"


# ── check_datetime_format ─────────────────────────────────────────────────────

def test_datetime_passes_iso8601():
    df = make_df(created_at=["2025-01-15T14:23:00Z", "2025-02-01T09:00:00Z"])
    props = make_props(created_at={"type": "string", "format": "date-time"})
    violations = []
    check_datetime_format(df, props, violations)
    assert violations == []


def test_datetime_fails_invalid_format():
    df = make_df(created_at=["01/15/2025", "not-a-date"])
    props = make_props(created_at={"type": "string", "format": "date-time"})
    violations = []
    check_datetime_format(df, props, violations)
    assert violations[0]["status"] == "FAIL"


# ── check_statistical_drift ───────────────────────────────────────────────────

def test_drift_no_baseline_returns_new_entry():
    df = make_df(score=[0.7, 0.8, 0.9, 0.85])
    props = make_props(score={"type": "number"})
    violations = []
    new = check_statistical_drift(df, props, baselines={}, violations=violations)
    assert "score" in new
    assert violations == []


def test_drift_pass_within_threshold():
    df = make_df(score=[0.80, 0.81, 0.79, 0.82])
    baselines = {"score": {"mean": 0.80, "stddev": 0.05}}
    violations = []
    check_statistical_drift(df, {}, baselines=baselines, violations=violations)
    assert violations == []


def test_drift_warn_at_2_sigma():
    # z = (0.91 - 0.80) / 0.05 = 2.2 → WARN (> 2 but <= 3)
    df = make_df(score=[0.91, 0.91, 0.91])
    baselines = {"score": {"mean": 0.80, "stddev": 0.05}}
    violations = []
    check_statistical_drift(df, {}, baselines=baselines, violations=violations)
    assert any(v["status"] == "WARN" for v in violations)


def test_drift_fail_at_3_sigma():
    df = make_df(score=[0.98, 0.97, 0.99])  # mean ~0.98, baseline 0.50, stddev 0.05
    baselines = {"score": {"mean": 0.50, "stddev": 0.05}}
    violations = []
    check_statistical_drift(df, {}, baselines=baselines, violations=violations)
    assert any(v["status"] == "FAIL" for v in violations)


# ── run_validation (integration) ──────────────────────────────────────────────

CLEAN_RECORDS = [
    {
        "doc_id": f"{'a' * 8}-{'b' * 4}-{'c' * 4}-{'d' * 4}-{'e' * 12}",
        "extracted_facts": [
            {"fact_id": f"{'f' * 8}-0000-0000-0000-{str(i).zfill(12)}",
             "confidence": 0.85, "page_ref": i}
        ],
        "processing_time_ms": 1000,
        "extracted_at": f"2025-01-{i+1:02d}T14:00:00Z",
    }
    for i in range(5)
]

SCALE_VIOLATED_RECORDS = [
    {
        "doc_id": f"{'a' * 8}-{'b' * 4}-{'c' * 4}-{'d' * 4}-{'e' * 12}",
        "extracted_facts": [
            {"fact_id": f"{'f' * 8}-0000-0000-0000-{str(i).zfill(12)}",
             "confidence": 85.0, "page_ref": i}
        ],
        "processing_time_ms": 1000,
        "extracted_at": f"2025-01-{i+1:02d}T14:00:00Z",
    }
    for i in range(5)
]


@pytest.fixture
def clean_setup(tmp_path):
    data_path = tmp_path / "clean.jsonl"
    with open(data_path, "w") as f:
        for r in CLEAN_RECORDS:
            f.write(json.dumps(r) + "\n")
    contract_path = generate(
        source=str(data_path),
        contract_id="test-clean",
        lineage=None,
        output_dir=str(tmp_path / "contracts"),
    )
    return str(contract_path), str(data_path), str(tmp_path)


@pytest.fixture
def violated_setup(tmp_path):
    # Generate contract from clean data first
    clean_path = tmp_path / "clean.jsonl"
    with open(clean_path, "w") as f:
        for r in CLEAN_RECORDS:
            f.write(json.dumps(r) + "\n")
    contract_path = generate(
        source=str(clean_path),
        contract_id="test-clean",
        lineage=None,
        output_dir=str(tmp_path / "contracts"),
    )
    # Violated data
    violated_path = tmp_path / "violated.jsonl"
    with open(violated_path, "w") as f:
        for r in SCALE_VIOLATED_RECORDS:
            f.write(json.dumps(r) + "\n")
    return str(contract_path), str(violated_path), str(tmp_path)


def test_validation_pass_on_clean_data(clean_setup, tmp_path):
    contract_path, data_path, base = clean_setup
    report_path = str(tmp_path / "report_clean.json")
    baselines_path = str(tmp_path / "baselines.json")
    report = run_validation(contract_path, data_path, report_path, baselines_path)
    assert report["overall_status"] == "PASS"
    assert report["violation_count"] == 0
    assert Path(report_path).exists()


def test_validation_fails_on_scale_violation(violated_setup, tmp_path):
    contract_path, violated_path, base = violated_setup
    report_path = str(tmp_path / "report_violated.json")
    # Write baselines from clean data first
    baselines_path = str(tmp_path / "baselines_v.json")
    clean_path = str(Path(violated_path).parent / "clean.jsonl")
    run_validation(contract_path, clean_path, str(tmp_path / "r0.json"), baselines_path)
    # Now validate violated data — must FAIL
    report = run_validation(contract_path, violated_path, report_path, baselines_path)
    assert report["overall_status"] == "FAIL"
    assert any(v["check"] == "range" for v in report["violations"])


def test_validation_report_written_to_disk(clean_setup, tmp_path):
    contract_path, data_path, _ = clean_setup
    report_path = str(tmp_path / "subdir" / "report.json")
    baselines_path = str(tmp_path / "baselines2.json")
    run_validation(contract_path, data_path, report_path, baselines_path)
    assert Path(report_path).exists()
    with open(report_path) as f:
        data = json.load(f)
    assert "overall_status" in data
    assert "violations" in data
