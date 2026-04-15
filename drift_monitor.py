"""
drift_monitor.py

Compares feature distributions between two weekly Delta snapshots
and flags columns where the distribution has shifted significantly.

For each monitored column, computes per-category frequency counts
and percentage shares in both snapshots, then calculates the
absolute change. Any category exceeding the configured threshold
is labelled as a significant drift.

Results are written to:
  - A summary table (current week, overwrite)
  - A history table (all weeks, upsert by variable + version)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from delta.tables import DeltaTable
from functools import reduce

from config import DRIFT_SUMMARY_TABLE, DRIFT_HISTORY_TABLE, DRIFT_THRESHOLD
from delta_utils import get_monday_snapshots, load_snapshot


def compute_drift_summary(
    spark: SparkSession,
    table_name: str,
    monitor_cols: list[str],
    rn1: int = 1,
    rn2: int = 2,
    threshold: float = DRIFT_THRESHOLD,
) -> DataFrame:
    """
    Computes distribution drift for each column in monitor_cols.

    For each column and each category value, produces:
      - Frequency and % share in both snapshots
      - Absolute change in % share (pct_diff)
      - Drift flag: 'Significant Change' if |pct_diff| > threshold

    Args:
        spark:        Active SparkSession.
        table_name:   Fully-qualified Delta table name.
        monitor_cols: Columns to include in drift analysis.
        rn1:          Latest snapshot row number (default 1).
        rn2:          Previous snapshot row number (default 2).
        threshold:    Minimum |Δ%| to flag as significant drift.

    Returns:
        Spark DataFrame with one row per (variable, category).
    """
    latest_meta, previous_meta = get_monday_snapshots(spark, table_name, rn1, rn2)

    latest_version   = latest_meta["version"]
    previous_version = previous_meta["version"]
    latest_week      = latest_meta["date"]
    previous_week    = previous_meta["date"]

    print(
        f"Comparing snapshots: "
        f"latest={latest_week} (v{latest_version}) | "
        f"previous={previous_week} (v{previous_version})"
    )

    latest_df   = load_snapshot(spark, table_name, latest_version)
    previous_df = load_snapshot(spark, table_name, previous_version)

    total_latest   = latest_df.count()
    total_previous = previous_df.count()

    results = []

    for col_name in monitor_cols:
        if col_name not in latest_df.columns or col_name not in previous_df.columns:
            print(f"  Skipping '{col_name}' — not present in both snapshots.")
            continue

        # Treat nulls as a distinct category for monitoring
        null_safe = f.when(
            f.col(col_name).isNull(), f.lit("(null)")
        ).otherwise(f.col(col_name).cast("string"))

        latest_dist   = latest_df.groupBy(null_safe.alias("category")).agg(
            f.count(f.lit(1)).alias("freq_latest")
        )
        previous_dist = previous_df.groupBy(null_safe.alias("category")).agg(
            f.count(f.lit(1)).alias("freq_previous")
        )

        merged = (
            latest_dist
            .join(previous_dist, on="category", how="full")
            .na.fill({"freq_latest": 0, "freq_previous": 0})
        )

        result = (
            merged
            .withColumn("pct_latest",
                f.round(f.col("freq_latest")   / total_latest   * 100, 4))
            .withColumn("pct_previous",
                f.round(f.col("freq_previous") / total_previous * 100, 4))
            .withColumn("freq_diff",
                f.col("freq_latest") - f.col("freq_previous"))
            .withColumn("pct_diff",
                f.round(f.col("pct_latest") - f.col("pct_previous"), 4))
            .withColumn("drift_flag",
                f.when(f.abs(f.col("pct_diff")) > threshold,
                       f.lit("Significant Change"))
                 .otherwise(f.lit("No Significant Change")))
            .withColumn("variable",          f.lit(col_name))
            .withColumn("latest_version",    f.lit(latest_version))
            .withColumn("previous_version",  f.lit(previous_version))
            .withColumn("latest_week",       f.lit(latest_week))
            .withColumn("previous_week",     f.lit(previous_week))
            .withColumn("run_timestamp",     f.current_timestamp())
        )
        results.append(result)

    if not results:
        raise RuntimeError("No drift results produced — verify monitor_cols.")

    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True), results
    )


def save_drift_results(spark: SparkSession, summary_df: DataFrame) -> None:
    """
    Persists drift summary results.

    Writes the current run to the summary table (overwrite) and
    upserts into the history table to avoid duplicates across reruns.

    Args:
        spark:      Active SparkSession.
        summary_df: Output of compute_drift_summary().
    """
    # Overwrite current week's summary
    (summary_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DRIFT_SUMMARY_TABLE))
    print(f"Drift summary written to {DRIFT_SUMMARY_TABLE}")

    # Upsert into history: delete matching (variable, version) then append
    if not spark.catalog.tableExists(DRIFT_HISTORY_TABLE):
        (summary_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(DRIFT_HISTORY_TABLE))
    else:
        dt = DeltaTable.forName(spark, DRIFT_HISTORY_TABLE)
        keys = summary_df.select("variable", "latest_version").distinct()
        keys.createOrReplaceTempView("_drift_keys")
        (dt.alias("t")
           .merge(
               spark.table("_drift_keys").alias("k"),
               "t.variable = k.variable AND t.latest_version = k.latest_version"
           )
           .whenMatchedDelete()
           .execute())
        (summary_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(DRIFT_HISTORY_TABLE))

    print(f"Drift history updated in {DRIFT_HISTORY_TABLE}")