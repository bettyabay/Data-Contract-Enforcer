"""Tests for contracts/runner.py (Phase 2 report schema)"""
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

CONTRACT_ID = "test-contract"


def make_df(**kwargs):
    return pd.DataFrame(kwargs)


def make_props(**kwargs):
    return kwargs


def collect_results():
    return []


# ── check_required ────────────────────────────────────────────────────────────

def test_required_passes_when_no_nulls():
    df = make_df(name=["Alice", "Bob"])
    props = make_props(name={"type": "string", "required": True})
    results = []
    check_required(df, props, CONTRACT_ID, results)
    assert results == []


def test_required_fails_when_nulls_present():
    df = make_df(name=["Alice", None])
    props = make_props(name={"type": "string", "required": True})
    results = []
    check_required(df, props, CONTRACT_ID, results)
    assert len(results) == 1
    assert results[0]["status"] == "CRITICAL"
    assert results[0]["check_type"] == "required_field_present"


def test_required_fails_when_field_absent():
    df = make_df(other=["x"])
    props = make_props(name={"type": "string", "required": True})
    results = []
    check_required(df, props, CONTRACT_ID, results)
    assert results[0]["status"] in ("CRITICAL", "ERROR")


# ── check_types ───────────────────────────────────────────────────────────────

def test_type_check_passes_numeric():
    df = make_df(score=[0.9, 0.7, 0.8])
    props = make_props(score={"type": "number"})
    results = []
    check_types(df, props, CONTRACT_ID, results)
    assert results == []


def test_type_check_fails_string_for_number():
    df = make_df(score=["high", "low", "medium"])
    props = make_props(score={"type": "number"})
    results = []
    check_types(df, props, CONTRACT_ID, results)
    assert results[0]["status"] == "CRITICAL"
    assert results[0]["check_type"] == "type_match"


# ── check_enum ────────────────────────────────────────────────────────────────

def test_enum_passes_all_valid():
    df = make_df(verdict=["PASS", "FAIL", "PASS"])
    props = make_props(verdict={"type": "string", "enum": ["PASS", "FAIL", "WARN"]})
    results = []
    check_enum(df, props, CONTRACT_ID, results)
    assert results == []


def test_enum_fails_invalid_value():
    df = make_df(verdict=["PASS", "INVALID", "FAIL"])
    props = make_props(verdict={"type": "string", "enum": ["PASS", "FAIL", "WARN"]})
    results = []
    check_enum(df, props, CONTRACT_ID, results)
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"
    assert "INVALID" in str(results[0].get("sample_failing", results[0].get("actual_value", "")))


# ── check_ranges ──────────────────────────────────────────────────────────────

def test_range_passes_within_bounds():
    df = make_df(confidence=[0.7, 0.8, 0.9])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    results = []
    check_ranges(df, props, CONTRACT_ID, results)
    assert results == []


def test_range_fails_above_maximum():
    df = make_df(confidence=[70.0, 80.0, 90.0])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    results = []
    check_ranges(df, props, CONTRACT_ID, results)
    assert len(results) == 1
    assert results[0]["status"] == "CRITICAL"
    assert results[0]["check_type"] == "range"


def test_range_fails_below_minimum():
    df = make_df(score=[-5.0, 2.0, 3.0])
    props = make_props(score={"type": "number", "minimum": 0.0, "maximum": 10.0})
    results = []
    check_ranges(df, props, CONTRACT_ID, results)
    assert results[0]["status"] == "CRITICAL"


# ── check_uuid_pattern ────────────────────────────────────────────────────────

def test_uuid_passes_valid_uuids():
    df = make_df(doc_id=[
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "b2c3d4e5-f6a7-8901-bcde-f12345678901",
    ])
    props = make_props(doc_id={"type": "string", "format": "uuid"})
    results = []
    check_uuid_pattern(df, props, CONTRACT_ID, results)
    assert results == []


def test_uuid_fails_invalid_value():
    df = make_df(doc_id=["not-a-uuid", "also-not-valid"])
    props = make_props(doc_id={"type": "string", "format": "uuid"})
    results = []
    check_uuid_pattern(df, props, CONTRACT_ID, results)
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"


# ── check_datetime_format ─────────────────────────────────────────────────────

def test_datetime_passes_iso8601():
    df = make_df(created_at=["2025-01-15T14:23:00Z", "2025-02-01T09:00:00Z"])
    props = make_props(created_at={"type": "string", "format": "date-time"})
    results = []
    check_datetime_format(df, props, CONTRACT_ID, results)
    assert results == []


def test_datetime_fails_invalid_format():
    df = make_df(created_at=["01/15/2025", "not-a-date"])
    props = make_props(created_at={"type": "string", "format": "date-time"})
    results = []
    check_datetime_format(df, props, CONTRACT_ID, results)
    assert results[0]["status"] == "FAIL"


# ── check_statistical_drift ───────────────────────────────────────────────────

def test_drift_no_baseline_returns_new_entry():
    df = make_df(score=[0.7, 0.8, 0.9, 0.85])
    props = make_props(score={"type": "number"})
    results = []
    new = check_statistical_drift(df, props, CONTRACT_ID, baselines={}, results=results)
    assert "score" in new
    assert results == []


def test_drift_pass_within_threshold():
    df = make_df(score=[0.80, 0.81, 0.79, 0.82])
    baselines = {"score": {"mean": 0.80, "stddev": 0.05}}
    results = []
    check_statistical_drift(df, {}, CONTRACT_ID, baselines=baselines, results=results)
    assert results == []


def test_drift_warn_at_2_sigma():
    # z = (0.91 - 0.80) / 0.05 = 2.2 → WARN
    df = make_df(score=[0.91, 0.91, 0.91])
    baselines = {"score": {"mean": 0.80, "stddev": 0.05}}
    results = []
    check_statistical_drift(df, {}, CONTRACT_ID, baselines=baselines, results=results)
    assert any(r["status"] == "WARN" for r in results)


def test_drift_fail_at_3_sigma():
    df = make_df(score=[0.98, 0.97, 0.99])
    baselines = {"score": {"mean": 0.50, "stddev": 0.05}}
    results = []
    check_statistical_drift(df, {}, CONTRACT_ID, baselines=baselines, results=results)
    assert any(r["status"] == "FAIL" for r in results)


# ── Phase 2 report schema checks ─────────────────────────────────────────────

def test_result_has_check_id():
    df = make_df(confidence=[70.0, 80.0])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    results = []
    check_ranges(df, props, "mycontract", results)
    assert "check_id" in results[0]
    assert results[0]["check_id"].startswith("mycontract.")


def test_result_has_records_failing():
    df = make_df(confidence=[70.0, 80.0])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    results = []
    check_ranges(df, props, CONTRACT_ID, results)
    assert "records_failing" in results[0]
    assert isinstance(results[0]["records_failing"], int)


def test_result_has_severity():
    df = make_df(confidence=[70.0, 80.0])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    results = []
    check_ranges(df, props, CONTRACT_ID, results)
    assert results[0]["severity"] == "CRITICAL"


def test_result_has_column_name():
    df = make_df(confidence=[70.0, 80.0])
    props = make_props(confidence={"type": "number", "minimum": 0.0, "maximum": 1.0})
    results = []
    check_ranges(df, props, CONTRACT_ID, results)
    assert results[0]["column_name"] == "confidence"


# ── run_validation (integration) ─────────────────────────────────────────────

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
    baselines_path = str(tmp_path / "baselines_v.json")
    clean_path = str(Path(violated_path).parent / "clean.jsonl")
    run_validation(contract_path, clean_path, str(tmp_path / "r0.json"), baselines_path)
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


def test_report_has_phase2_fields(clean_setup, tmp_path):
    """Phase 2: report must have report_id, snapshot_id, total_checks, passed, failed."""
    contract_path, data_path, _ = clean_setup
    report_path = str(tmp_path / "report_p2.json")
    baselines_path = str(tmp_path / "baselines_p2.json")
    report = run_validation(contract_path, data_path, report_path, baselines_path)
    assert "report_id" in report
    assert "snapshot_id" in report
    assert "total_checks" in report
    assert "passed" in report
    assert "failed" in report
    assert "warned" in report
    assert "errored" in report


def test_report_results_have_check_id(clean_setup, tmp_path):
    """Phase 2: every result entry must have check_id, column_name, severity."""
    contract_path, data_path, _ = clean_setup
    report_path = str(tmp_path / "report_p2b.json")
    baselines_path = str(tmp_path / "baselines_p2b.json")
    # Run twice so drift results also appear
    run_validation(contract_path, data_path, str(tmp_path / "r0.json"), baselines_path)
    report = run_validation(contract_path, data_path, report_path, baselines_path)
    for r in report.get("results", []):
        assert "check_id" in r, f"result missing check_id: {r}"
        assert "column_name" in r, f"result missing column_name: {r}"
        assert "severity" in r, f"result missing severity: {r}"
        assert "records_failing" in r, f"result missing records_failing: {r}"
