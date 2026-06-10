"""
dag_invoice_monitor_client_xyz.py
---------------------------------
Airflow DAG — ClientXYZ Invoice Anomaly Monitor (Client XYZ)

Pipeline stages:
  1. generate_data      : Generate/refresh synthetic invoice data (simulates SQL Server export)
  2. validate_raw_data  : Check file exists and has expected schema/row count
  3. run_spark_job      : Submit PySpark anomaly detection job via SparkSubmitOperator
  4. generate_report    : Summarise results and write a markdown report
  5. alert_on_anomalies : Fail the DAG (or send alert) if anomaly rate exceeds threshold

Schedule: Daily at 06:00 BRT
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Paths (inside Docker volumes)
# ---------------------------------------------------------------------------
BASE_DIR = Path("/opt/airflow")
RAW_CSV = BASE_DIR / "data" / "raw" / "invoices_client_xyz.csv"
PROCESSED_DIR = BASE_DIR / "data" / "processed" / "invoice_anomalies"
REPORT_DIR = BASE_DIR / "data" / "processed" / "reports"
SPARK_JOB = BASE_DIR / "spark_jobs" / "invoice_anomaly_detection.py"

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
ANOMALY_RATE_THRESHOLD = 0.10  # Alert if >10% of records are anomalous

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def _generate_data(**context):
    """Run the data generator script to produce fresh synthetic invoices."""
    script = BASE_DIR / "scripts" / "generate_invoices.py"
    result = subprocess.run(
        ["python", str(script)],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)
    context["ti"].xcom_push(key="generator_output", value=result.stdout)


def _validate_raw_data(**context):
    """
    Validate:
      - CSV file exists
      - Has required columns
      - Row count > 0
    """
    import csv

    if not RAW_CSV.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_CSV}")

    with open(RAW_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required_cols = {
            "invoice_id", "case_id", "client_id", "amount",
            "import_date", "status", "source_system", "batch_id"
        }
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        rows = sum(1 for _ in reader)

    if rows == 0:
        raise ValueError("Raw CSV is empty")

    print(f"Validation passed — {rows} rows, all required columns present")
    context["ti"].xcom_push(key="raw_row_count", value=rows)


def _generate_report(**context):
    """
    Read the Parquet output, compute anomaly metrics, write a markdown report.
    Uses pandas for simplicity inside Airflow worker (no Spark needed here).
    """
    import pandas as pd

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Read all anomaly_type partitions
    frames = []
    for part_dir in PROCESSED_DIR.iterdir():
        if part_dir.is_dir() and part_dir.name.startswith("anomaly_type="):
            anomaly_type = part_dir.name.split("=")[1]
            parquet_files = list(part_dir.glob("*.parquet"))
            if parquet_files:
                df_part = pd.read_parquet(part_dir)
                df_part["anomaly_type"] = anomaly_type
                frames.append(df_part)

    if not frames:
        raise ValueError("No Parquet partitions found in processed dir")

    df = pd.concat(frames, ignore_index=True)
    total = len(df)
    anomalous = len(df[df["anomaly_type"] != "CLEAN"])
    anomaly_rate = anomalous / total if total > 0 else 0

    summary = df.groupby("anomaly_type").agg(
        record_count=("invoice_id", "count"),
        total_amount=("amount", "sum"),
        unique_invoices=("invoice_id", "nunique"),
    ).reset_index()

    run_date = context["ds"]
    report_path = REPORT_DIR / f"report_{run_date}.md"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# ClientXYZ Invoice Anomaly Report\n\n")
        f.write(f"**Client:** XYZ  \n")
        f.write(f"**Run date:** {run_date}  \n")
        f.write(f"**Total records:** {total}  \n")
        f.write(f"**Anomalous records:** {anomalous} ({anomaly_rate:.1%})  \n\n")
        f.write("## Breakdown by Anomaly Type\n\n")
        f.write("| Anomaly Type | Records | Unique Invoices | Total Amount (R$) |\n")
        f.write("|---|---|---|---|\n")
        for _, row in summary.iterrows():
            f.write(
                f"| {row['anomaly_type']} "
                f"| {row['record_count']} "
                f"| {row['unique_invoices']} "
                f"| {row['total_amount']:,.2f} |\n"
            )
        f.write("\n## Anomaly Definitions\n\n")
        f.write("- **EXACT_DUPLICATE**: Same invoice imported more than once with identical data. Root cause: ANT batch deduplication failure.\n")
        f.write("- **AMOUNT_MISMATCH**: Same invoice imported with different amounts across source systems. Root cause: Dental Par sent correction without voiding original.\n")
        f.write("- **CASE_MISMATCH**: Same invoice linked to different debtor cases. Root cause: incorrect case reference on re-import.\n")
        f.write("- **CLEAN**: No anomaly detected.\n")

    print(f"Report written to: {report_path}")
    context["ti"].xcom_push(key="anomaly_rate", value=anomaly_rate)
    context["ti"].xcom_push(key="anomalous_count", value=anomalous)


def _branch_on_anomaly_rate(**context):
    """Branch: if anomaly rate > threshold, go to alert task; else go to success."""
    ti = context["ti"]
    anomaly_rate = ti.xcom_pull(task_ids="generate_report", key="anomaly_rate")
    if anomaly_rate is None:
        return "alert_anomaly_threshold_exceeded"
    return (
        "alert_anomaly_threshold_exceeded"
        if anomaly_rate > ANOMALY_RATE_THRESHOLD
        else "pipeline_success"
    )


def _alert_on_anomalies(**context):
    """
    In production: send email / Slack / Teams alert.
    Here: raise an exception to mark the DAG run as failed for visibility.
    """
    ti = context["ti"]
    rate = ti.xcom_pull(task_ids="generate_report", key="anomaly_rate")
    count = ti.xcom_pull(task_ids="generate_report", key="anomalous_count")
    raise ValueError(
        f"[ALERT] Client XYZ ClientXYZ — anomaly rate {rate:.1%} exceeds threshold "
        f"{ANOMALY_RATE_THRESHOLD:.0%}. {count} anomalous records detected. "
        f"Review data/processed/reports/ for details."
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="invoice_monitor_client_xyz",
    description="ClientXYZ (Client XYZ) invoice anomaly detection pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",  # 06:00 daily
    catchup=False,
    default_args=default_args,
    tags=["debt-collection", "client_xyz", "invoice", "data-quality"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=_generate_data,
    )

    validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=_validate_raw_data,
    )

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command=(
            f"docker exec invoice-monitor-spark-master-1 "
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"/opt/spark_jobs/invoice_anomaly_detection.py "
            f"--input /opt/data/raw/invoices_client_xyz.csv "
            f"--output /opt/data/processed/invoice_anomalies"
        ),
        # When running inside the Airflow container, submit directly:
        # bash_command=(
        #     f"spark-submit --master local[*] "
        #     f"{SPARK_JOB} "
        #     f"--input {RAW_CSV} "
        #     f"--output {PROCESSED_DIR}"
        # ),
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )

    branch = BranchPythonOperator(
        task_id="check_anomaly_rate",
        python_callable=_branch_on_anomaly_rate,
    )

    alert = PythonOperator(
        task_id="alert_anomaly_threshold_exceeded",
        python_callable=_alert_on_anomalies,
    )

    success = EmptyOperator(
        task_id="pipeline_success",
    )

    # ---------------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------------
    generate_data >> validate_raw_data >> run_spark_job >> generate_report >> branch
    branch >> [alert, success]
