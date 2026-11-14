# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Bronze: incremental ingestion with Auto Loader
# MAGIC
# MAGIC **Bronze** = raw data, kept as close to the source as possible, plus audit columns. We never clean here – if a
# MAGIC downstream rule is wrong we can rebuild from bronze.
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) is Databricks' file-ingestion engine:
# MAGIC * discovers *only new* files each run (state kept in a **checkpoint**)
# MAGIC * infers and tracks the schema, handles new columns (**schema evolution**)
# MAGIC * puts unparseable fields into `_rescued_data` instead of failing
# MAGIC
# MAGIC `trigger(availableNow=True)` = "process everything that's new, then stop" – streaming semantics, batch cost.
# MAGIC It is also what lets us run streaming on serverless/Free Edition.

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

BRONZE_TABLE = "bronze_trips"

def ingest_bronze():
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
    q = (
        stream.writeStream
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )
    q.awaitTermination()

ingest_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT _source_file) AS files FROM bronze_trips

# COMMAND ----------

display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prove it is incremental
# MAGIC 1. Note the row count above.
# MAGIC 2. Run the first cell again → **no new rows** (the checkpoint remembers every file already read).
# MAGIC 3. Open notebook **04**, set the widget to **2**, run it → new files land.
# MAGIC 4. Come back and run `ingest_bronze()` again → only batch 2's files are added.

# COMMAND ----------

ingest_bronze()
display(spark.sql("SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT _source_file) AS files FROM bronze_trips"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the table history
# MAGIC Each micro-batch is a commit with `STREAMING UPDATE`:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_trips

# COMMAND ----------

# MAGIC %md
# MAGIC ## `COPY INTO`: the simpler SQL alternative
# MAGIC For modest file volumes, an idempotent SQL load (re-running skips files already loaded):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_copyinto;
# MAGIC COPY INTO bronze_copyinto
# MAGIC FROM '/Volumes/<catalog>/<schema>/landing/trips'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC ```
# MAGIC Prefer Auto Loader for large/continuous directories (better file discovery at scale).
# MAGIC
# MAGIC ## Exercises
# MAGIC 1. Which columns exist in bronze beyond the original six? (`DESCRIBE bronze_trips`)
# MAGIC 2. Count rows per `_source_file` – what do the files look like?
# MAGIC 3. Delete the checkpoint folder (`dbutils.fs.rm(..., True)`) and the table, re-run: why does this re-read everything?
