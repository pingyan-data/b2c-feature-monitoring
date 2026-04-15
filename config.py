"""
config.py

Central configuration for the B2C feature monitoring pipeline.
Replace placeholder paths with your actual Databricks table paths
before running. Do not commit real paths to version control.
"""

# ── Source table ───────────────────────────────────────────────
B2C_FEATURE_TABLE = "your_catalog.your_schema.b2c_feature_table"

# ── Output tables ──────────────────────────────────────────────
DRIFT_SUMMARY_TABLE  = "your_catalog.your_schema.b2c_drift_summary"
DRIFT_HISTORY_TABLE  = "your_catalog.your_schema.b2c_drift_history"
CROSSTAB_TABLE       = "your_catalog.your_schema.b2c_crosstab"

# ── Column naming conventions ──────────────────────────────────
# Columns starting with these prefixes are included in monitoring
HOUSEHOLD_PREFIX   = "hh_"
INDIVIDUAL_PREFIX  = "ind_"
CUSTOMER_ID_COL    = "customer_id"

# Columns ending with these suffixes are excluded (raw scores/values)
EXCLUDE_SUFFIXES   = ("_value", "_scr")

# Additional columns to exclude explicitly
EXCLUDE_COLS_EXTRA = [
    "customer_id",
    "household_id",
]

# ── Drift detection settings ───────────────────────────────────
DRIFT_THRESHOLD     = 5.0   # Flag if |Δ%| exceeds this value
MIN_CARDINALITY     = 2     # Min distinct values for crosstab
MAX_CARDINALITY     = 20    # Max distinct values for crosstab