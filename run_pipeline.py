"""
run_pipeline.py

Entry point for the weekly B2C feature monitoring pipeline.

Attach this file to a Databricks scheduled job, or paste the
contents into a Databricks notebook and run on a weekly schedule.

Steps:
  1. Load the B2C feature table and select columns to monitor.
  2. Compute distribution drift vs. the previous weekly snapshot.
  3. Compute categorical transition crosstabs.
  4. Write results to Delta output tables.
"""

from pyspark.sql import SparkSession

from config import (
    B2C_FEATURE_TABLE,
    HOUSEHOLD_PREFIX,
    INDIVIDUAL_PREFIX,
    EXCLUDE_SUFFIXES,
    EXCLUDE_COLS_EXTRA,
)
from delta_utils import select_monitor_columns, load_snapshot
from drift_monitor import compute_drift_summary, save_drift_results
from crosstab_monitor import compute_crosstab, save_crosstab

spark = SparkSession.builder.getOrCreate()

# ── 1. Select columns to monitor ──────────────────────────────
feature_df   = spark.table(B2C_FEATURE_TABLE)
monitor_cols = select_monitor_columns(
    df=feature_df,
    household_prefix=HOUSEHOLD_PREFIX,
    individual_prefix=INDIVIDUAL_PREFIX,
    exclude_suffixes=EXCLUDE_SUFFIXES,
    exclude_extra=EXCLUDE_COLS_EXTRA,
)
print(f"Monitoring {len(monitor_cols)} feature columns.")

# ── 2. Distribution drift ──────────────────────────────────────
drift_df = compute_drift_summary(
    spark=spark,
    table_name=B2C_FEATURE_TABLE,
    monitor_cols=monitor_cols,
    rn1=1,
    rn2=2,
)
save_drift_results(spark, drift_df)

# ── 3. Categorical transition crosstab ─────────────────────────
crosstab_df = compute_crosstab(
    spark=spark,
    table_name=B2C_FEATURE_TABLE,
    rn1=1,
    rn2=2,
)
save_crosstab(crosstab_df)

print("Weekly B2C feature monitoring complete.")