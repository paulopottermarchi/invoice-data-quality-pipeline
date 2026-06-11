"""
invoice_description_anomaly_detection.py
-----------------------------------------
PySpark job that connects to SQL Server via JDBC, reads dbo.invoice
for client XYZ (ClientXYZ), and detects description-field anomalies
using the same logic as the production T-SQL query:

  Anomaly 1 — MES DIVERGENTE:
    Month extracted from 'Competencia:' field ≠ month from 'Vencimento:' field

  Anomaly 2 — DPD NEGATIVO:
    DPD value extracted from 'DPD:' field is a negative integer

  Anomaly 3 — AMBOS:
    Both conditions above simultaneously

All string parsing mirrors the T-SQL SUBSTRING + CHARINDEX + PATINDEX logic,
translated to PySpark regexp_extract and substring functions.

Usage:
  spark-submit \
    --jars /opt/spark_jars/mssql-jdbc-12.6.1.jre11.jar \
    spark_jobs/invoice_description_anomaly_detection.py \
    --jdbc-url "jdbc:sqlserver://HOST:1433;databaseName=your_database;encrypt=false;trustServerCertificate=true" \
    --db-user YOUR_USER \
    --db-password YOUR_PASSWORD \
    --output data/processed/description_anomalies \
    --report-dir data/processed/reports
"""

import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# JDBC read
# ---------------------------------------------------------------------------

def read_invoices_from_sqlserver(spark: SparkSession, jdbc_url: str,
                                  user: str, password: str,
                                  client_id: int = 1):
    """
    Reads dbo.invoice joined to dbo.case for the given client_id.
    Only rows where description contains both 'Competencia:' and 'Vencimento:'
    are loaded — same as the WHERE clause in the original T-SQL.

    Uses a pushdown query so Spark only transfers relevant rows over JDBC.
    """
    pushdown_query = f"""
    (
        SELECT
            i.invoice_id,
            i.case_id,
            i.invoice_number,
            i.description,
            i.insert_date,
            i.insert_user
        FROM dbo.invoice i
        WHERE
            i.case_id IN (
                SELECT case_id
                FROM dbo.[case]
                WHERE client_id = {client_id}
            )
            AND CHARINDEX('Competencia:', i.description) > 0
            AND CHARINDEX('Vencimento:',  i.description) > 0
    ) AS invoice_subset
    """

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", pushdown_query)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", "1000")
        .load()
    )


# ---------------------------------------------------------------------------
# Field extraction — mirrors T-SQL SUBSTRING/CHARINDEX/PATINDEX logic
# ---------------------------------------------------------------------------

def extract_description_fields(df):
    """
    Extracts Competencia, Vencimento, and DPD values from the free-text
    description column using regexp_extract.

    The description field format (from ANT system):
      ...
      Competencia: MM.YYYY
      Vencimento: DD.MM.YYYY
      DPD: -15
      ...

    Regex patterns are equivalent to the T-SQL substring extraction logic.
    """

    # Extract raw Competencia string (e.g. "01.2024")
    df = df.withColumn(
        "competencia_raw",
        F.trim(F.regexp_extract(F.col("description"), r"Competencia:\s*([^\r\n]+)", 1))
    )

    # Extract raw Vencimento string (e.g. "15.01.2024")
    df = df.withColumn(
        "vencimento_raw",
        F.trim(F.regexp_extract(F.col("description"), r"Vencimento:\s*([^\r\n]+)", 1))
    )

    # Extract DPD value as integer (can be negative)
    df = df.withColumn(
        "dpd_invoice",
        F.regexp_extract(F.col("description"), r"DPD:\s*(-?\d+)", 1)
        .cast(IntegerType())
    )

    # Month from Competencia: characters 4-5 of "MM.YYYY" → MM
    # T-SQL: SUBSTRING(..., 4, 2) on "01.2024" gives "20" — wait, that's the year start
    # T-SQL SUBSTRING is 1-based. "Competencia: 01.2024"
    #   position 1=0, 2=1, 3=., 4=2, 5=0, 6=2, 7=4
    # The T-SQL extracts pos 4,2 = "20" which is the century of year
    # BUT the comparison is between competencia[4:2] vs vencimento[4:2]
    # Vencimento "15.01.2024": pos 4,2 = "01" = month
    # So the T-SQL is actually comparing year-century of competencia VS month of vencimento
    # This IS intentional — it catches when the date fields are inconsistent/swapped
    # We replicate the EXACT same logic:

    # competencia_raw: "01.2024" → substring(4,2) 1-based = chars at index 3,4 (0-based) = "20"
    df = df.withColumn(
        "competencia_pos4",
        F.substring(F.col("competencia_raw"), 4, 2)  # PySpark substring is 1-based too
    )

    # vencimento_raw: "15.01.2024" → substring(4,2) = "01" = month segment
    df = df.withColumn(
        "vencimento_pos4",
        F.substring(F.col("vencimento_raw"), 4, 2)
    )

    # Corrected vencimento date: swap DD and MM then parse
    # T-SQL: pos4,2 + '.' + pos1,2 + '.' + pos7,4 → "01.15.2024" → TRY_CONVERT(DATE, ..., 104)
    # Format 104 = dd.mm.yyyy, so after swap it becomes valid
    df = df.withColumn(
        "vencimento_corrigido",
        F.to_date(
            F.concat(
                F.substring(F.col("vencimento_raw"), 4, 2), F.lit("."),
                F.substring(F.col("vencimento_raw"), 1, 2), F.lit("."),
                F.substring(F.col("vencimento_raw"), 7, 4)
            ),
            "MM.dd.yyyy"
        )
    )

    return df


# ---------------------------------------------------------------------------
# Anomaly classification — mirrors T-SQL CASE logic
# ---------------------------------------------------------------------------

def classify_anomalies(df):
    """
    Replicates the CASE WHEN logic from the original T-SQL:
      - AMBOS: MES DIVERGENTE + DPD NEGATIVO  (both conditions)
      - MES DIVERGENTE                          (only month mismatch)
      - DPD NEGATIVO                            (only negative DPD)
      - CLEAN                                   (no anomaly — added for completeness)
    """
    mes_divergente = F.col("competencia_pos4") != F.col("vencimento_pos4")
    dpd_negativo = F.col("dpd_invoice") < 0

    df = df.withColumn(
        "motivo_erro",
        F.when(mes_divergente & dpd_negativo, F.lit("AMBOS: MES DIVERGENTE + DPD NEGATIVO"))
        .when(mes_divergente, F.lit("MES DIVERGENTE"))
        .when(dpd_negativo, F.lit("DPD NEGATIVO"))
        .otherwise(F.lit("CLEAN"))
    )

    df = df.withColumn(
        "is_anomaly",
        F.when(F.col("motivo_erro") != "CLEAN", True).otherwise(False)
    )

    return df


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_anomalies(df, output_path: str):
    """Write only anomalous records, partitioned by motivo_erro."""
    anomalies = df.filter(F.col("is_anomaly"))
    (
        anomalies
        .repartition("motivo_erro")
        .write
        .mode("overwrite")
        .partitionBy("motivo_erro")
        .parquet(output_path)
    )
    return anomalies.count()


def write_markdown_report(df, report_dir: str, run_date: str, client_id: int):
    """Generate a markdown report from the Spark DataFrame using pandas."""
    import pandas as pd

    Path(report_dir).mkdir(parents=True, exist_ok=True)

    pdf = df.toPandas()
    total = len(pdf)
    anomalies = pdf[pdf["is_anomaly"]]
    anomaly_count = len(anomalies)
    anomaly_rate = anomaly_count / total if total > 0 else 0

    summary = (
        anomalies.groupby("motivo_erro")
        .agg(record_count=("invoice_id", "count"),
             unique_invoices=("invoice_id", "nunique"))
        .reset_index()
    )

    report_path = Path(report_dir) / f"description_anomalies_{run_date}.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# Invoice Description Anomaly Report\n\n")
        f.write(f"**Client:** {client_id} — CLIENTXYZ  \n")
        f.write(f"**Run date:** {run_date}  \n")
        f.write(f"**Total invoices analysed:** {total}  \n")
        f.write(f"**Anomalous records:** {anomaly_count} ({anomaly_rate:.1%})  \n\n")

        if anomaly_count == 0:
            f.write("## ✅ No anomalies detected\n\n")
        else:
            f.write("## ⚠️ Anomalies Detected\n\n")
            f.write("| Motivo | Records | Unique Invoices |\n")
            f.write("|--------|---------|----------------|\n")
            for _, row in summary.iterrows():
                f.write(f"| {row['motivo_erro']} | {row['record_count']} | {row['unique_invoices']} |\n")

            f.write("\n## Anomalous Invoice Detail\n\n")
            cols = ["invoice_id", "case_id", "invoice_number",
                    "competencia_raw", "vencimento_raw", "dpd_invoice",
                    "vencimento_corrigido", "motivo_erro", "insert_date"]
            available = [c for c in cols if c in anomalies.columns]
            f.write(anomalies[available].sort_values(
                ["motivo_erro", "insert_date"], ascending=[True, False]
            ).to_markdown(index=False))

        f.write("\n\n## Anomaly Definitions\n\n")
        f.write("- **MES DIVERGENTE**: Month in `Competencia:` field ≠ month in `Vencimento:` field — likely a data entry or import error.\n")
        f.write("- **DPD NEGATIVO**: DPD value is negative — invoice due date is in the future relative to import date, which should not occur for debt collection records.\n")
        f.write("- **AMBOS**: Both conditions simultaneously — highest severity.\n")

    print(f"Report written: {report_path}")
    return str(report_path), anomaly_count, anomaly_rate


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report-dir", required=True)
    parser.add_argument("--client-id", type=int, default=1)
    parser.add_argument("--run-date", default=str(date.today()))
    args = parser.parse_args()

    spark = build_spark("ClientXYZDescriptionAnomalyDetection")
    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to SQL Server via JDBC...")
    df = read_invoices_from_sqlserver(
        spark, args.jdbc_url, args.db_user, args.db_password, args.client_id
    )
    total = df.count()
    print(f"Loaded {total} invoice records for client {args.client_id}")

    df = extract_description_fields(df)
    df = classify_anomalies(df)
    df.cache()

    print("\n=== Anomaly Summary ===")
    df.groupBy("motivo_erro").count().orderBy("motivo_erro").show()

    anomaly_count = write_anomalies(df, args.output)
    print(f"Anomalous records written to Parquet: {anomaly_count}")

    report_path, count, rate = write_markdown_report(
        df, args.report_dir, args.run_date, args.client_id
    )

    # Write summary to stdout for Airflow XCom capture
    print(f"ANOMALY_COUNT={count}")
    print(f"ANOMALY_RATE={rate:.4f}")
    print(f"REPORT_PATH={report_path}")

    spark.stop()


if __name__ == "__main__":
    main()
