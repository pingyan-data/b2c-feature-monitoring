"""
crosstab_monitor.py

Builds a category transition matrix for each categorical feature
between two weekly Delta snapshots.

For each customer and each categorical column, records how the
value changed from the previous snapshot to the latest. Output is
a long-format table suited for heatmap visualisation in a
Databricks SQL dashboard or BI tool.

Transition types:
  stayed           — value unchanged
  moved            — value changed to a different category
  new_in_latest    — customer has a value now but had none before
  missing_in_latest — customer had a value before but has none now
  both_null        — null in both snapshots
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from config import (
    CROSSTAB_TABLE,
    CUSTOMER_ID_COL,
    MIN_CARDINALITY,
    MAX_CARDINALITY,
)
from delta_utils import get_monday_snapshots, load_snapshot


def _detect_categorical_columns(
    latest_df: DataFrame,
    previous_df: DataFrame,
    common_cols: list[str],
    min_cardinality: int,
    max_cardinality: int,
) -> list[str]:
    """
    Identifies categorical columns based on approximate distinct counts.

    A column qualifies if its maximum distinct count across both
    snapshots falls within [min_cardinality, max_cardinality).

    Args:
        latest_df:       Latest snapshot DataFrame.
        previous_df:     Previous snapshot DataFrame.
        common_cols:     Columns present in both snapshots.
        min_cardinality: Minimum number of distinct values.
        max_cardinality: Maximum number of distinct values (exclusive).

    Returns:
        List of qualifying column names.
    """
    def distinct_counts(df: DataFrame) -> dict:
        return (
            df.agg(*[f.approx_count_distinct(c).alias(c) for c in common_cols])
            .collect()[0]
            .asDict()
        )

    latest_counts   = distinct_counts(latest_df)
    previous_counts = distinct_counts(previous_df)

    return [
        c for c in common_cols
        if min_cardinality
           <= max(latest_counts.get(c, 0), previous_counts.get(c, 0))
           < max_cardinality
    ]


def _to_long_format(
    df: DataFrame,
    id_col: str,
    categorical_cols: list[str],
    value_alias: str,
) -> DataFrame:
    """
    Converts a wide DataFrame to long format for efficient joining.

    Each row in the output represents one (customer, variable, value)
    tuple. This avoids separate per-column scans when processing
    many columns.

    Args:
        df:               Source DataFrame.
        id_col:           Customer identifier column.
        categorical_cols: Columns to pivot into long format.
        value_alias:      Name for the value column in the output.

    Returns:
        Long-format DataFrame with columns: id_col, variable, value_alias.
    """
    kv_structs = [
        f.struct(
            f.lit(c).alias("variable"),
            f.col(c).cast("string").alias(value_alias),
        )
        for c in categorical_cols
    ]
    return (
        df.select(id_col, f.explode(f.array(*kv_structs)).alias("kv"))
          .select(
              f.col(id_col),
              f.col("kv.variable").alias("variable"),
              f.col(f"kv.{value_alias}").alias(value_alias),
          )
    )


def compute_crosstab(
    spark: SparkSession,
    table_name: str,
    id_col: str = CUSTOMER_ID_COL,
    rn1: int = 1,
    rn2: int = 2,
    min_cardinality: int = MIN_CARDINALITY,
    max_cardinality: int = MAX_CARDINALITY,
) -> DataFrame:
    """
    Computes a long-format transition crosstab between two snapshots.

    For each qualifying categorical column and each customer, records
    the previous and latest category values and derives the transition
    type. Outputs row- and column-normalised percentages for heatmap
    visualisation.

    Args:
        spark:           Active SparkSession.
        table_name:      Fully-qualified Delta table name.
        id_col:          Customer identifier column.
        rn1:             Latest snapshot row number (default 1).
        rn2:             Previous snapshot row number (default 2).
        min_cardinality: Min distinct values to qualify as categorical.
        max_cardinality: Max distinct values to qualify as categorical.

    Returns:
        Spark DataFrame with columns:
        variable, previous_value, latest_value, count,
        pct_of_previous, pct_of_latest, transition_type,
        latest_version, previous_version, latest_week, previous_week,
        created_at.
    """
    latest_meta, previous_meta = get_monday_snapshots(spark, table_name, rn1, rn2)

    latest_version   = int(latest_meta["version"])
    previous_version = int(previous_meta["version"])
    latest_week      = latest_meta["date"]
    previous_week    = previous_meta["date"]

    print(
        f"Comparing snapshots: "
        f"latest={latest_week} (v{latest_version}) | "
        f"previous={previous_week} (v{previous_version})"
    )

    latest_df   = load_snapshot(spark, table_name, latest_version)
    previous_df = load_snapshot(spark, table_name, previous_version)

    if id_col not in latest_df.columns or id_col not in previous_df.columns:
        raise ValueError(f"id_col='{id_col}' must exist in both snapshots.")

    common_cols = sorted(
        (set(latest_df.columns) & set(previous_df.columns)) - {id_col}
    )

    categorical_cols = _detect_categorical_columns(
        latest_df, previous_df, common_cols, min_cardinality, max_cardinality
    )
    print(f"Categorical columns detected: {len(categorical_cols)}")

    if not categorical_cols:
        raise RuntimeError("No categorical columns matched the cardinality filter.")

    latest_long   = _to_long_format(latest_df,   id_col, categorical_cols, "latest_value")
    previous_long = _to_long_format(previous_df, id_col, categorical_cols, "previous_value")

    joined = latest_long.join(
        previous_long, on=[id_col, "variable"], how="full_outer"
    )

    counts = (
        joined
        .groupBy("variable", "previous_value", "latest_value")
        .count()
        .withColumn("latest_version",   f.lit(latest_version))
        .withColumn("previous_version", f.lit(previous_version))
        .withColumn("latest_week",      f.lit(latest_week))
        .withColumn("previous_week",    f.lit(previous_week))
        .withColumn("created_at",       f.current_timestamp())
    )

    w_prev   = Window.partitionBy("variable", "previous_value")
    w_latest = Window.partitionBy("variable", "latest_value")

    return (
        counts
        .withColumn("prev_total", f.sum("count").over(w_prev))
        .withColumn("pct_of_previous",
            f.round(
                f.when(f.col("prev_total") > 0,
                       f.col("count") / f.col("prev_total") * 100)
                 .otherwise(0.0), 2
            ))
        .drop("prev_total")
        .withColumn("transition_type",
            f.when(
                f.col("previous_value").isNull() & f.col("latest_value").isNotNull(),
                f.lit("new_in_latest"))
             .when(
                f.col("previous_value").isNotNull() & f.col("latest_value").isNull(),
                f.lit("missing_in_latest"))
             .when(
                f.col("previous_value").isNull() & f.col("latest_value").isNull(),
                f.lit("both_null"))
             .when(
                f.col("previous_value") == f.col("latest_value"),
                f.lit("stayed"))
             .otherwise(f.lit("moved"))
        )
        .withColumn("latest_total", f.sum("count").over(w_latest))
        .withColumn("pct_of_latest",
            f.round(
                f.when(f.col("latest_total") > 0,
                       f.col("count") / f.col("latest_total") * 100)
                 .otherwise(0.0), 2
            ))
        .drop("latest_total")
    )


def save_crosstab(
    crosstab_df: DataFrame,
    target_table: str = CROSSTAB_TABLE,
) -> None:
    """
    Writes the crosstab DataFrame to a Delta table, partitioned by variable.

    Args:
        crosstab_df:  Output of compute_crosstab().
        target_table: Target Delta table name.
    """
    (crosstab_df
        .repartition("variable")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))
    print(f"Crosstab saved to {target_table}")