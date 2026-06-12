"""
test_invoice_anomaly_detection.py
-----------------------------------
Unit tests for the PySpark anomaly detection logic.
Uses a local SparkSession (no cluster needed).

Run: pytest tests/test_invoice_anomaly_detection.py -v
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from spark_jobs.invoice_anomaly_detection import (
    detect_exact_duplicates,
    detect_amount_mismatch,
    detect_case_mismatch,
    classify_anomaly,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_invoice_anomaly_detection")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_df(spark, rows):
    columns = ["invoice_id", "case_id", "client_id", "amount",
               "import_date", "status", "source_system", "batch_id"]
    return spark.createDataFrame(rows, columns)


# ---------------------------------------------------------------------------
# Exact duplicate tests
# ---------------------------------------------------------------------------

def test_exact_duplicate_detected(spark):
    rows = [
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-02", "IMPORTED", "ANT", "B002"),
        ("INV-002", "CASE-2", 516, 300.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
    ]
    df = make_df(spark, rows)
    result = detect_exact_duplicates(df)
    flags = {row["invoice_id"]: row["exact_duplicate_flag"]
             for row in result.collect()}
    assert flags["INV-001"] is True
    assert flags["INV-002"] is False


def test_no_false_positive_exact_duplicate(spark):
    rows = [
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
        ("INV-002", "CASE-2", 516, 300.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
    ]
    df = make_df(spark, rows)
    result = detect_exact_duplicates(df)
    for row in result.collect():
        assert row["exact_duplicate_flag"] is False


# ---------------------------------------------------------------------------
# Amount mismatch tests
# ---------------------------------------------------------------------------

def test_amount_mismatch_detected(spark):
    rows = [
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
        ("INV-001", "CASE-1", 516, 450.0, "2024-01-02", "IMPORTED", "DENTAL_PAR", "B002"),
        ("INV-002", "CASE-2", 516, 300.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
    ]
    df = make_df(spark, rows)
    result = detect_amount_mismatch(df)
    flags = {row["invoice_id"]: row["amount_mismatch_flag"]
             for row in result.collect()}
    assert flags["INV-001"] is True
    assert flags["INV-002"] is False


# ---------------------------------------------------------------------------
# Case mismatch tests
# ---------------------------------------------------------------------------

def test_case_mismatch_detected(spark):
    rows = [
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
        ("INV-001", "CASE-9", 516, 500.0, "2024-01-02", "IMPORTED", "ANT", "B002"),
        ("INV-002", "CASE-2", 516, 300.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
    ]
    df = make_df(spark, rows)
    result = detect_case_mismatch(df)
    flags = {row["invoice_id"]: row["case_mismatch_flag"]
             for row in result.collect()}
    assert flags["INV-001"] is True
    assert flags["INV-002"] is False


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------

def test_classify_case_mismatch_priority(spark):
    """CASE_MISMATCH takes priority over EXACT_DUPLICATE."""
    rows = [
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
        ("INV-001", "CASE-9", 516, 500.0, "2024-01-02", "IMPORTED", "ANT", "B002"),
    ]
    df = make_df(spark, rows)
    df = detect_exact_duplicates(df)
    df = detect_amount_mismatch(df)
    df = detect_case_mismatch(df)
    df = classify_anomaly(df)
    types = {row["anomaly_type"] for row in df.collect()}
    assert "CASE_MISMATCH" in types
    assert "EXACT_DUPLICATE" not in types


def test_classify_clean(spark):
    rows = [
        ("INV-001", "CASE-1", 516, 500.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
        ("INV-002", "CASE-2", 516, 300.0, "2024-01-01", "IMPORTED", "ANT", "B001"),
    ]
    df = make_df(spark, rows)
    df = detect_exact_duplicates(df)
    df = detect_amount_mismatch(df)
    df = detect_case_mismatch(df)
    df = classify_anomaly(df)
    for row in df.collect():
        assert row["anomaly_type"] == "CLEAN"
