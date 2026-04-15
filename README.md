# B2C Feature Monitoring Pipeline

Weekly data quality monitoring for B2C customer feature tables in a
Databricks + Delta Lake environment.

Detects distribution shifts and category transitions between weekly
snapshots — providing early warning before upstream data changes
silently degrade model performance.

---

## Business context

B2C marketing models rely on a feature table of customer attributes
covering demographics, behavioural scores, and household-level data.
When upstream data pipelines change — or the customer population
shifts — model inputs can drift without any visible error, causing
prediction quality to degrade silently.

This pipeline runs weekly, comparing the latest feature snapshot
against the previous week using Delta Lake's built-in versioning.
Results feed a Databricks SQL dashboard that allows data scientists
and data engineers to spot and investigate drift before it affects
live models.

---

## What it detects

**Distribution drift** monitors every feature column and flags any
category whose share of the population has shifted by more than 5
percentage points week-over-week.

**Transition crosstab** shows, for each categorical feature, exactly
how individual customers moved between category values — making it
easy to distinguish a genuine population shift from a data pipeline
error such as a mass reclassification.

---

## Project structure

    b2c-feature-monitoring/
    ├── config.py            # Table paths and monitoring settings
    ├── delta_utils.py       # Delta version history + snapshot loading
    ├── drift_monitor.py     # Distribution comparison and drift flagging
    ├── crosstab_monitor.py  # Category transition matrix
    └── run_pipeline.py      # Entry point — run as a Databricks job

---

## Architecture

---

## Setup

**1. Configure paths** in `config.py`:

```python
B2C_FEATURE_TABLE   = "your_catalog.your_schema.b2c_feature_table"
DRIFT_SUMMARY_TABLE = "your_catalog.your_schema.b2c_drift_summary"
DRIFT_HISTORY_TABLE = "your_catalog.your_schema.b2c_drift_history"
CROSSTAB_TABLE      = "your_catalog.your_schema.b2c_crosstab"
```

**2. Adjust column conventions** if your table uses different prefixes:

```python
HOUSEHOLD_PREFIX  = "hh_"
INDIVIDUAL_PREFIX = "ind_"
EXCLUDE_SUFFIXES  = ("_value", "_scr")
```

**3. Schedule the pipeline** by attaching `run_pipeline.py` to a
Databricks job with a weekly Monday trigger, or paste it into a
notebook and run manually.

**4. Connect a dashboard** — the output tables are designed for
direct connection to Databricks SQL, Power BI, or Tableau.

---

## Output tables

### `b2c_drift_summary` — current week

One row per (feature column × category value).

| Column | Description |
|---|---|
| `variable` | Feature column name |
| `category` | Category value (or `(null)`) |
| `freq_latest` | Count in latest snapshot |
| `freq_previous` | Count in previous snapshot |
| `pct_latest` | % share in latest snapshot |
| `pct_previous` | % share in previous snapshot |
| `pct_diff` | Change in percentage points |
| `drift_flag` | `Significant Change` or `No Significant Change` |
| `latest_week` | Date of latest snapshot |
| `previous_week` | Date of previous snapshot |

### `b2c_drift_history` — all weeks

Same schema as `b2c_drift_summary`, appended weekly.
Upserted by `(variable, latest_version)` to prevent duplicate runs.

### `b2c_crosstab` — current week

One row per (feature column × previous value × latest value).

| Column | Description |
|---|---|
| `variable` | Feature column name |
| `previous_value` | Category in previous snapshot |
| `latest_value` | Category in latest snapshot |
| `count` | Customers with this transition |
| `pct_of_previous` | % of previous group moving here |
| `pct_of_latest` | % of latest group coming from there |
| `transition_type` | `stayed` / `moved` / `new_in_latest` / `missing_in_latest` |

---

## Requirements

- Databricks Runtime 12.x or later
- Delta Lake (included in Databricks Runtime)
- PySpark (included in Databricks Runtime)

---

## License

MIT