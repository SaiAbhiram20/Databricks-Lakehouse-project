# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Silver: clean, validate, de-duplicate
# MAGIC
# MAGIC **Silver** = trustworthy, typed, de-duplicated data. Rules here:
# MAGIC 1. **Cast** to proper types (bronze may hold strings).
# MAGIC 2. **Validate** with explicit business rules. Bad rows are **quarantined with a reason**, never silently dropped.
# MAGIC 3. **De-duplicate** on the business key.
# MAGIC 4. **Upsert with MERGE** so re-runs are idempotent (running twice gives the same result).

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

bronze = spark.table("bronze_trips")

typed = bronze.select(
    "trip_id",
    F.to_timestamp("tpep_pickup_datetime").alias("pickup_ts"),
    F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_ts"),
    F.col("trip_distance").cast("double").alias("trip_distance"),
    F.col("fare_amount").cast("double").alias("fare_amount"),
    F.col("pickup_zip").cast("int").alias("pickup_zip"),
    F.col("dropoff_zip").cast("int").alias("dropoff_zip"),
    F.to_date("pickup_date").alias("pickup_date"),
    "_source_file",
    "_ingested_at",
)

# One expression defines every rule. NULL means "passes all rules".
reject_reason = (
    F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "missing_timestamp")
    .when(F.col("fare_amount") <= 0, "non_positive_fare")
    .when(F.col("trip_distance") < 0, "negative_distance")
    .when(F.col("dropoff_ts") < F.col("pickup_ts"), "dropoff_before_pickup")
)

valid = typed.filter(reject_reason.isNull())
rejected = typed.withColumn("reject_reason", reject_reason).filter(F.col("reject_reason").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine the rejects (overwritten each run because we re-read all of bronze)

# COMMAND ----------

rejected.write.mode("overwrite").saveAsTable("silver_quarantine")
display(spark.sql("SELECT reject_reason, COUNT(*) AS n FROM silver_quarantine GROUP BY reject_reason"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-duplicate and MERGE into silver

# COMMAND ----------

deduped = valid.dropDuplicates(["trip_id"])
print("valid rows:", valid.count(), "-> after de-dup:", deduped.count())

if not spark.catalog.tableExists("silver_trips"):
    deduped.limit(0).write.saveAsTable("silver_trips")        # create empty table with the right schema

deduped.createOrReplaceTempView("silver_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_trips AS t
# MAGIC USING silver_updates AS s
# MAGIC ON t.trip_id = s.trip_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_rows, COUNT(DISTINCT trip_id) AS distinct_ids FROM silver_trips

# COMMAND ----------

# MAGIC %md
# MAGIC `silver_rows = distinct_ids` proves the duplicates are gone. **Re-run the MERGE cell**: the count doesn't change –
# MAGIC that's idempotency.
# MAGIC
# MAGIC > Caveat: because `trip_id` hashes the business columns, two *genuinely different* trips with identical values
# MAGIC > collapse into one. Real sources should supply a true unique id; here it's a deliberate simplification.
# MAGIC
# MAGIC ## Reconcile: nothing lost
# MAGIC bronze rows = silver rows + quarantined rows + duplicates removed.

# COMMAND ----------

display(spark.sql("""
SELECT
  (SELECT COUNT(*) FROM bronze_trips)      AS bronze,
  (SELECT COUNT(*) FROM silver_trips)      AS silver,
  (SELECT COUNT(*) FROM silver_quarantine) AS quarantined
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC 1. Add a rule rejecting trips longer than 6 hours (`unknown_long_trip`). How many rows does it catch?
# MAGIC 2. Add a derived column `duration_min` to silver.
# MAGIC 3. Make this incremental: read bronze with `_ingested_at > (max in silver)` instead of the whole table.
