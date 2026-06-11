"""
invoice_anomaly_detection.py
-----------------------------
PySpark job that reads raw invoice data, detects anomalies, and writes
classified results to the processed layer as Parquet partitioned by anomaly_type.

Anomaly types detected:
  - EXACT_DUPLICATE      : same invoice_id, case_id, amount — imported more than once
  - AMOUNT_MISMATCH      : same invoice_id, same case_id, different amounts across sources
  - CASE_MISMATCH        : same invoice_id linked to different case_ids
  - CLEAN                : no anomaly detected

Usage (local):
  spark-submit spark_jobs/invoice_anomaly_detection.py \
    --input data/raw/invoices_client_xyz.csv \
    --output data/processed/invoice_anomalies

Usage (Docker Spark):
  docker exec -it invoice-monitor-spark-master-1 \
    spark-submit /opt/spark_jobs/invoice_anomaly_detection.py \
      --input /opt/data/raw/invoices_client_xyz.csv \
      --output /opt/data/processed/invoice_anomalies
"""

import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # small dataset local tuning
        .getOrCreate()
    )


def read_invoices(spark: SparkSession, input_path: str):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )


def detect_exact_duplicates(df):
    """
    Exact duplicate: same invoice_id + case_id + amount appearing more than once.
    Root cause: ANT importing the same batch twice without deduplication guard.
    """
    window = Window.partitionBy("invoice_id", "case_id", "amount")
    return (
        df.withColumn("_dup_count", F.count("*").over(window))
        .withColumn(
            "exact_duplicate_flag",
            F.when(F.col("_dup_count") > 1, True).otherwise(False)
        )
        .drop("_dup_count")
    )


def detect_amount_mismatch(df):
    """
    Amount mismatch: same invoice_id + case_id but different amounts across source systems.
    Root cause: ANT imported one value; Dental Par sent correction with different value.
    """
    amount_per_invoice = (
        df.groupBy("invoice_id", "case_id")
        .agg(F.countDistinct("amount").alias("distinct_amounts"))
    )
    return df.join(
        amount_per_invoice.withColumn(
            "amount_mismatch_flag",
            F.when(F.col("distinct_amounts") > 1, True).otherwise(False)
        ).drop("distinct_amounts"),
        on=["invoice_id", "case_id"],
        how="left"
    )


def detect_case_mismatch(df):
    """
    Case mismatch: same invoice_id linked to more than one case_id.
    Root cause: invoice re-imported with wrong case reference.
    """
    cases_per_invoice = (
        df.groupBy("invoice_id")
        .agg(F.countDistinct("case_id").alias("distinct_cases"))
    )
    return df.join(
        cases_per_invoice.withColumn(
            "case_mismatch_flag",
            F.when(F.col("distinct_cases") > 1, True).otherwise(False)
        ).drop("distinct_cases"),
        on="invoice_id",
        how="left"
    )


def classify_anomaly(df):
    """
    Derives a single anomaly_type column from the individual flags.
    Priority: CASE_MISMATCH > AMOUNT_MISMATCH > EXACT_DUPLICATE > CLEAN
    """
    return df.withColumn(
        "anomaly_type",
        F.when(F.col("case_mismatch_flag"), F.lit("CASE_MISMATCH"))
        .when(F.col("amount_mismatch_flag"), F.lit("AMOUNT_MISMATCH"))
        .when(F.col("exact_duplicate_flag"), F.lit("EXACT_DUPLICATE"))
        .otherwise(F.lit("CLEAN"))
    ).drop("exact_duplicate_flag", "amount_mismatch_flag", "case_mismatch_flag")


def write_results(df, output_path: str):
    (
        df.repartition("anomaly_type")
        .write
        .mode("overwrite")
        .partitionBy("anomaly_type")
        .parquet(output_path)
    )


def print_summary(df):
    print("\n=== Invoice Anomaly Summary ===")
    df.groupBy("anomaly_type").count().orderBy("anomaly_type").show()

    print("=== Amount exposure by anomaly type ===")
    (
        df.groupBy("anomaly_type")
        .agg(
            F.count("*").alias("record_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .orderBy("anomaly_type")
        .show()
    )


def main():
    parser = argparse.ArgumentParser(description="ClientXYZ Invoice Anomaly Detection")
    parser.add_argument("--input", required=True, help="Path to raw CSV")
    parser.add_argument("--output", required=True, help="Path for Parquet output")
    args = parser.parse_args()

    spark = build_spark_session("ClientXYZInvoiceAnomalyDetection")
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading invoices from: {args.input}")
    df = read_invoices(spark, args.input)

    print(f"Total records loaded: {df.count()}")

    df = detect_exact_duplicates(df)
    df = detect_amount_mismatch(df)
    df = detect_case_mismatch(df)
    df = classify_anomaly(df)

    df.cache()

    print_summary(df)

    print(f"\nWriting results to: {args.output}")
    write_results(df, args.output)

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
