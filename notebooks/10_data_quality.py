# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Data quality checks and monitoring
# MAGIC
# MAGIC Trust comes from **measuring** quality on every run. Pattern used here:
# MAGIC 1. Define rules as SQL that returns the **number of violations** (0 = pass).
# MAGIC 2. Append results to a `dq_results` history table (trend over time, alert on regressions).
# MAGIC 3. Fail the run if a *critical* rule is violated – so a Job stops before bad data reaches dashboards.

# COMMAND ----------

# ---- Shared config: identical at the top of every notebook. Change CATALOG here if needed. ----
from pyspark.sql import functions as F

CATALOG = "workspace"          # <- change if your workspace uses another catalog
SCHEMA = "learn_databricks"    # everything this guide creates lives here (easy to delete later)
VOLUME = "landing"             # a Unity Catalog volume = a governed folder for files

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/trips"          # raw JSON arrives here
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/_checkpoints"  # streaming progress is stored here
SOURCE_TABLE = "samples.nyctaxi.trips"                                 # built-in sample data

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Working in {CATALOG}.{SCHEMA}")
print(f"Landing path : {LANDING_PATH}")

# COMMAND ----------

dbutils.widgets.dropdown("fail_on_critical", "false", ["true", "false"], "Fail on critical violations")
FAIL_ON_CRITICAL = dbutils.widgets.get("fail_on_critical") == "true"

# COMMAND ----------

RULES = [
    # (rule name, severity, SQL returning violation count)
    ("silver_unique_trip_id", "critical", "SELECT COUNT(*) - COUNT(DISTINCT trip_id) FROM silver_trips"),
    ("silver_no_null_pickup", "critical", "SELECT COUNT(*) FROM silver_trips WHERE pickup_ts IS NULL"),
    ("silver_positive_fare", "critical", "SELECT COUNT(*) FROM silver_trips WHERE fare_amount <= 0"),
    ("silver_dropoff_after_pickup", "critical", "SELECT COUNT(*) FROM silver_trips WHERE dropoff_ts < pickup_ts"),
    ("silver_plausible_fare", "warning", "SELECT COUNT(*) FROM silver_trips WHERE fare_amount > 500"),
    ("gold_not_empty", "critical", "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM gold_daily_metrics"),
    ("quarantine_rate_under_5pct", "warning",
     "SELECT CASE WHEN (SELECT COUNT(*) FROM silver_quarantine) > 0.05 * (SELECT COUNT(*) FROM bronze_trips) THEN 1 ELSE 0 END"),
]

# COMMAND ----------

from datetime import datetime

run_ts = datetime.utcnow()
rows = []
for name, severity, sql_text in RULES:
    violations = int(spark.sql(sql_text).first()[0])
    rows.append((run_ts, name, severity, violations, violations == 0))

results = spark.createDataFrame(rows, "run_ts timestamp, rule string, severity string, violations long, passed boolean")
results.write.mode("append").saveAsTable("dq_results")
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## History: one query shows quality over time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rule, severity, COUNT(*) AS runs, SUM(CASE WHEN passed THEN 0 ELSE 1 END) AS failed_runs, MAX(run_ts) AS last_run
# MAGIC FROM dq_results
# MAGIC GROUP BY rule, severity
# MAGIC ORDER BY failed_runs DESC

# COMMAND ----------

failed_critical = [r for r in rows if r[2] == "critical" and not r[4]]
if failed_critical and FAIL_ON_CRITICAL:
    raise Exception(f"Critical data-quality rules failed: {[r[1] for r in failed_critical]}")
print("Critical failures:", [r[1] for r in failed_critical] or "none")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Break something on purpose
# MAGIC Insert a bad row into silver, re-run the cells above with `fail_on_critical = true`, watch it fail, then fix with `DELETE`:
# MAGIC ```sql
# MAGIC INSERT INTO silver_trips VALUES ('bad1', current_timestamp(), current_timestamp() - INTERVAL 1 HOUR, 1.0, -5.0, 10001, 10002, current_date(), 'manual', current_timestamp());
# MAGIC DELETE FROM silver_trips WHERE trip_id = 'bad1';
# MAGIC ```
# MAGIC
# MAGIC ## Alerting
# MAGIC In the **SQL Editor**, save: `SELECT COUNT(*) FROM dq_results WHERE NOT passed AND severity='critical' AND run_ts > current_timestamp() - INTERVAL 1 DAY`,
# MAGIC then **Alerts → Create** with condition "value > 0" and a notification (email/Slack). In Jobs (chapter 11) you can also
# MAGIC notify on task failure — the `fail_on_critical` flag turns data issues into task failures.
# MAGIC
# MAGIC ## Exercises
# MAGIC 1. Add a freshness rule: latest `pickup_date` in silver must be within N days of the max in bronze.
# MAGIC 2. Add a row-count reconciliation rule bronze vs (silver + quarantine + duplicates).
# MAGIC 3. Compare these hand-rolled rules with Lakeflow expectations (chapter 8). Which would you choose per case?
