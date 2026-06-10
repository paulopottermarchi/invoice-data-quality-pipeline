"""
dag_description_anomaly_monitor.py
------------------------------------
Airflow DAG — Invoice Description Anomaly Monitor (Client XYZ / ClientXYZ)

Connects directly to the company SQL Server via JDBC, runs a PySpark job
that replicates the production T-SQL anomaly detection query, and sends
an email alert if any anomalies are found.

Pipeline:
  1. check_sqlserver_connection  : Verify SQL Server is reachable before submitting Spark job
  2. run_spark_anomaly_detection : SparkSubmit job via JDBC → detects MES DIVERGENTE / DPD NEGATIVO
  3. parse_spark_output          : Extract anomaly count and report path from Spark stdout
  4. branch_on_anomalies         : If count >= threshold → send_alert_email, else pipeline_success
  5. send_alert_email            : Airflow EmailOperator with anomaly summary
  6. pipeline_success            : EmptyOperator — all clear

Schedule: Daily at 07:00 BRT (10:00 UTC)
"""

from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Configuration (read from environment / Airflow Variables)
# ---------------------------------------------------------------------------
BASE_DIR = Path("/opt/airflow")
SPARK_JOB = BASE_DIR / "spark_jobs" / "invoice_description_anomaly_detection.py"
JDBC_JAR = "/opt/spark_jars/mssql-jdbc-12.6.1.jre11.jar"
OUTPUT_DIR = BASE_DIR / "data" / "processed" / "description_anomalies"
REPORT_DIR = BASE_DIR / "data" / "processed" / "reports"

SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "localhost")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_DB = os.getenv("SQLSERVER_DATABASE", "your_database")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "")
JDBC_URL = (
    f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
    f"databaseName={SQLSERVER_DB};encrypt=false;trustServerCertificate=true"
)

ALERT_RECIPIENTS = os.getenv("ALERT_RECIPIENTS", "your_email@example.com").split(",")
ANOMALY_THRESHOLD = int(os.getenv("ANOMALY_ALERT_THRESHOLD", "1"))
CLIENT_ID = int(os.getenv("CLIENT_ID", "1"))

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,  # We handle alerting manually via EmailOperator
    "email_on_retry": False,
}


# ---------------------------------------------------------------------------
# Task: Check SQL Server connectivity
# ---------------------------------------------------------------------------
def _check_sqlserver_connection(**context):
    """
    Attempts a lightweight TCP connection to SQL Server before submitting
    the Spark job. Fails fast with a clear error if unreachable.
    """
    import socket
    host = SQLSERVER_HOST
    port = int(SQLSERVER_PORT)
    timeout = 10

    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        print(f"✅ SQL Server reachable at {host}:{port}")
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        raise ConnectionError(
            f"❌ Cannot reach SQL Server at {host}:{port}. "
            f"Check VPN/network and SQLSERVER_HOST env var. Error: {e}"
        )


# ---------------------------------------------------------------------------
# Task: Run Spark job via subprocess (BashOperator alternative with XCom)
# ---------------------------------------------------------------------------
def _run_spark_job(**context):
    """
    Submits the PySpark job and captures stdout to extract anomaly metrics
    for downstream tasks via XCom.
    """
    run_date = context["ds"]

    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--jars", JDBC_JAR,
        str(SPARK_JOB),
        "--jdbc-url", JDBC_URL,
        "--db-user", SQLSERVER_USER,
        "--db-password", SQLSERVER_PASSWORD,
        "--output", str(OUTPUT_DIR),
        "--report-dir", str(REPORT_DIR),
        "--client-id", str(CLIENT_ID),
        "--run-date", run_date,
    ]

    print(f"Running: spark-submit ... invoice_description_anomaly_detection.py --run-date {run_date}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("STDERR:", result.stderr[-3000:])  # last 3k chars to avoid log flood
        raise RuntimeError(f"Spark job failed with exit code {result.returncode}")

    print(result.stdout)
    context["ti"].xcom_push(key="spark_stdout", value=result.stdout)


# ---------------------------------------------------------------------------
# Task: Parse Spark stdout to extract metrics
# ---------------------------------------------------------------------------
def _parse_spark_output(**context):
    """
    Reads the spark stdout from XCom and extracts:
      - ANOMALY_COUNT
      - ANOMALY_RATE
      - REPORT_PATH
    Pushes each as a separate XCom key for downstream tasks.
    """
    ti = context["ti"]
    stdout = ti.xcom_pull(task_ids="run_spark_job", key="spark_stdout") or ""

    def extract(pattern, text, cast=str, default="0"):
        match = re.search(pattern, text)
        return cast(match.group(1)) if match else cast(default)

    anomaly_count = extract(r"ANOMALY_COUNT=(\d+)", stdout, int)
    anomaly_rate = extract(r"ANOMALY_RATE=([\d.]+)", stdout, float)
    report_path = extract(r"REPORT_PATH=(.+)", stdout, str, "")

    ti.xcom_push(key="anomaly_count", value=anomaly_count)
    ti.xcom_push(key="anomaly_rate", value=anomaly_rate)
    ti.xcom_push(key="report_path", value=report_path)

    print(f"Anomaly count : {anomaly_count}")
    print(f"Anomaly rate  : {anomaly_rate:.1%}")
    print(f"Report path   : {report_path}")


# ---------------------------------------------------------------------------
# Task: Branch based on anomaly count
# ---------------------------------------------------------------------------
def _branch_on_anomalies(**context):
    ti = context["ti"]
    count = ti.xcom_pull(task_ids="parse_spark_output", key="anomaly_count") or 0
    print(f"Anomaly count = {count}, threshold = {ANOMALY_THRESHOLD}")
    return "send_alert_email" if count >= ANOMALY_THRESHOLD else "pipeline_success"


# ---------------------------------------------------------------------------
# Task: Build email body dynamically
# ---------------------------------------------------------------------------
def _build_email_body(**context):
    """
    Reads the markdown report and converts it to a plain HTML email body.
    """
    ti = context["ti"]
    count = ti.xcom_pull(task_ids="parse_spark_output", key="anomaly_count") or 0
    rate = ti.xcom_pull(task_ids="parse_spark_output", key="anomaly_rate") or 0.0
    report_path = ti.xcom_pull(task_ids="parse_spark_output", key="report_path") or ""
    run_date = context["ds"]

    report_content = ""
    if report_path and Path(report_path).exists():
        with open(report_path, "r", encoding="utf-8") as f:
            report_content = f.read()

    html = f"""
    <html><body style="font-family: Arial, sans-serif; color: #222;">
    <h2 style="color: #c0392b;">⚠️ Invoice Anomaly Alert — Client XYZ (ClientXYZ)</h2>
    <table style="border-collapse:collapse; margin-bottom:16px;">
      <tr><td style="padding:4px 12px 4px 0;"><b>Run date</b></td><td>{run_date}</td></tr>
      <tr><td style="padding:4px 12px 4px 0;"><b>Anomalous records</b></td>
          <td style="color:#c0392b;font-weight:bold;">{count}</td></tr>
      <tr><td style="padding:4px 12px 4px 0;"><b>Anomaly rate</b></td><td>{rate:.1%}</td></tr>
    </table>

    <h3>Anomaly definitions</h3>
    <ul>
      <li><b>MES DIVERGENTE</b>: Month in <code>Competencia:</code> field ≠ month in <code>Vencimento:</code></li>
      <li><b>DPD NEGATIVO</b>: DPD value is negative (due date in the future)</li>
      <li><b>AMBOS</b>: Both conditions simultaneously — highest severity</li>
    </ul>

    <h3>Full report</h3>
    <pre style="background:#f4f4f4;padding:12px;border-radius:4px;font-size:13px;
                overflow:auto;max-height:500px;">{report_content}</pre>

    <hr/>
    <p style="color:#888;font-size:12px;">
      Generated by Invoice Anomaly Monitor · Airflow DAG: invoice_description_anomaly_monitor<br/>
      Report file: {report_path}
    </p>
    </body></html>
    """

    ti.xcom_push(key="email_html", value=html)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="invoice_description_anomaly_monitor",
    description="Monitors ClientXYZ invoice description fields for date/DPD anomalies",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",   # 10:00 UTC = 07:00 BRT
    catchup=False,
    default_args=default_args,
    tags=["debt-collection", "client_xyz", "invoice", "data-quality", "real-time"],
) as dag:

    check_connection = PythonOperator(
        task_id="check_sqlserver_connection",
        python_callable=_check_sqlserver_connection,
    )

    run_spark = PythonOperator(
        task_id="run_spark_job",
        python_callable=_run_spark_job,
        execution_timeout=timedelta(minutes=30),
    )

    parse_output = PythonOperator(
        task_id="parse_spark_output",
        python_callable=_parse_spark_output,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_anomalies",
        python_callable=_branch_on_anomalies,
    )

    build_email = PythonOperator(
        task_id="build_email_body",
        python_callable=_build_email_body,
    )

    send_email = EmailOperator(
        task_id="send_alert_email",
        to=ALERT_RECIPIENTS,
        subject="[ALERTA] Anomalias em invoices ClientXYZ (Cliente XYZ) — {{ ds }}",
        html_content="{{ task_instance.xcom_pull(task_ids='build_email_body', key='email_html') }}",
    )

    success = EmptyOperator(
        task_id="pipeline_success",
    )

    # ---------------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------------
    check_connection >> run_spark >> parse_output >> branch
    branch >> build_email >> send_email
    branch >> success
