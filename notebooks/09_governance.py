# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Governance with Unity Catalog
# MAGIC
# MAGIC Unity Catalog answers: **who can see what, who changed it, and where did it come from?**
# MAGIC
# MAGIC | Feature | Purpose |
# MAGIC |---|---|
# MAGIC | Privileges (`GRANT`/`REVOKE`) | Control access to catalogs, schemas, tables, volumes |
# MAGIC | Column masks | Hide sensitive values from non-privileged users |
# MAGIC | Row filters | Show different rows to different users |
# MAGIC | Tags & comments | Classify and document data |
# MAGIC | Lineage & audit | Automatic upstream/downstream tracking |
# MAGIC
# MAGIC > **Free Edition note:** you are the only user, so you can't *see* another person's view. We demonstrate the
# MAGIC > mechanics and use group checks (`is_account_group_member`) that return false for groups you're not in.
# MAGIC > Some statements may be unavailable in your workspace; each cell catches the error and says so.

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

def attempt(sql_text):
    """Run SQL, but explain instead of crashing if the feature isn't available."""
    try:
        return spark.sql(sql_text)
    except Exception as e:
        print("Not available here ->", str(e).splitlines()[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Privileges
# MAGIC Privileges are **inherited down** the hierarchy and need `USE CATALOG` + `USE SCHEMA` on the parents.

# COMMAND ----------

display(spark.sql("SHOW GRANTS ON TABLE gold_daily_metrics"))

# COMMAND ----------

# Share the gold table with every user in the account, then take it back.
attempt("GRANT SELECT ON TABLE gold_daily_metrics TO `account users`")
display(attempt("SHOW GRANTS ON TABLE gold_daily_metrics"))
attempt("REVOKE SELECT ON TABLE gold_daily_metrics FROM `account users`")

# COMMAND ----------

# MAGIC %md
# MAGIC **Principle:** analysts get `SELECT` on **gold** only; engineers write to bronze/silver; service principals run jobs.
# MAGIC
# MAGIC ## 2. Tags and comments

# COMMAND ----------

attempt("ALTER TABLE silver_trips SET TAGS ('layer' = 'silver', 'domain' = 'mobility')")
attempt("ALTER TABLE silver_trips ALTER COLUMN pickup_zip COMMENT 'ZIP code where the trip started'")
display(attempt(f"SELECT * FROM {CATALOG}.information_schema.table_tags WHERE schema_name = '{SCHEMA}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column mask and row filter
# MAGIC Logic is a SQL function evaluated **per query, per user**. We apply, test, and remove them in one cell so they
# MAGIC can't break later chapters (a row filter that hides everything from you would).

# COMMAND ----------

attempt("""
CREATE OR REPLACE FUNCTION mask_zip(zip INT)
RETURN CASE WHEN is_account_group_member('zip_admins') THEN zip ELSE NULL END
""")
attempt("""
CREATE OR REPLACE FUNCTION manhattan_only(zip INT)
RETURN is_account_group_member('zip_admins') OR (zip BETWEEN 10001 AND 10282)
""")

try:
    attempt("ALTER TABLE silver_trips ALTER COLUMN pickup_zip SET MASK mask_zip")
    print("--- with column mask (you are not in zip_admins, so pickup_zip is NULL) ---")
    display(spark.sql("SELECT trip_id, pickup_zip, fare_amount FROM silver_trips LIMIT 5"))
finally:
    attempt("ALTER TABLE silver_trips ALTER COLUMN pickup_zip DROP MASK")

try:
    attempt("ALTER TABLE silver_trips SET ROW FILTER manhattan_only ON (pickup_zip)")
    print("--- with row filter (only Manhattan-range ZIPs visible) ---")
    display(spark.sql("SELECT COUNT(*) AS visible_rows, MIN(pickup_zip) AS min_zip, MAX(pickup_zip) AS max_zip FROM silver_trips"))
finally:
    attempt("ALTER TABLE silver_trips DROP ROW FILTER")

# COMMAND ----------

# MAGIC %md
# MAGIC In a real workspace you would create a group (`zip_admins`) in the account console, add people, and verify
# MAGIC members see raw values while everyone else doesn't.
# MAGIC
# MAGIC ## 4. Lineage
# MAGIC Open **Catalog → `gold_daily_metrics` → Lineage → See lineage graph**. You'll see silver and bronze upstream, and columns
# MAGIC traced end-to-end. No instrumentation needed – Unity Catalog captures it from query execution.
# MAGIC
# MAGIC ## 5. Audit via system tables (if enabled)

# COMMAND ----------

display(attempt("""
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name LIKE '%learn_databricks%'
ORDER BY event_time DESC LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC 1. Design a privilege model for three roles: *analyst*, *data engineer*, *auditor*. Write the `GRANT`s.
# MAGIC 2. Change the mask to show only the first 3 digits of a ZIP (hint: return `STRING`, cast the column).
# MAGIC 3. Find out what `GRANT ALL PRIVILEGES` includes, and why least-privilege is safer.
