# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Land raw files (simulate a real source)
# MAGIC
# MAGIC Real pipelines start from **files that arrive over time**, not a tidy table. This notebook turns the sample table
# MAGIC into JSON files in a Unity Catalog volume, in **two batches**, and deliberately adds dirty data:
# MAGIC
# MAGIC * negative fares, drop-off before pick-up (bad values → must be quarantined later)
# MAGIC * exact duplicate rows (must be de-duplicated later)
# MAGIC
# MAGIC Use the **batch** widget at the top: run with `1` now; run again with `2` in chapter 5 to see *incremental* ingestion.

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

dbutils.widgets.dropdown("batch", "1", ["1", "2"], "Batch to land")
batch = int(dbutils.widgets.get("batch"))
print("Landing batch", batch)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a stable `trip_id`
# MAGIC The source has no key, so we hash the business columns. Identical rows get the same id → duplicates are detectable.

# COMMAND ----------

KEY_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"]

def with_id(df):
    return df.withColumn("trip_id", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in KEY_COLS]), 256))

base = spark.table(SOURCE_TABLE).withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))

# split the calendar in half: first half = batch 1, second half = batch 2
dates = sorted(r[0] for r in base.select("pickup_date").distinct().collect())
cutoff = dates[len(dates) // 2]
print("days:", len(dates), "| batch 1 = before", cutoff, "| batch 2 = from", cutoff)

part = base.filter(F.col("pickup_date") < F.lit(cutoff)) if batch == 1 else base.filter(F.col("pickup_date") >= F.lit(cutoff))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inject dirty records

# COMMAND ----------

seed = batch * 10

bad_fare = part.sample(0.003, seed=seed + 1).withColumn("fare_amount", -F.abs(F.col("fare_amount")))

bad_time = (
    part.sample(0.003, seed=seed + 2)
    .withColumn("tpep_dropoff_datetime", F.col("tpep_pickup_datetime") - F.expr("INTERVAL 10 MINUTES"))
)

good = with_id(part)
dirty = with_id(bad_fare).unionByName(with_id(bad_time))
duplicates = good.sample(0.004, seed=seed + 3)          # same trip_id appears twice

out = good.unionByName(dirty).unionByName(duplicates)
print("clean:", good.count(), "| dirty:", dirty.count(), "| duplicates:", duplicates.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write JSON files to the volume
# MAGIC `partitionBy("pickup_date")` creates `pickup_date=2016-01-01/…` folders. Auto Loader will read the folder name
# MAGIC back as a column. Mode `append` = new files arrive next to old ones.

# COMMAND ----------

out.write.mode("append").partitionBy("pickup_date").json(LANDING_PATH)

# COMMAND ----------

files = [f.path for f in dbutils.fs.ls(LANDING_PATH)]
print(len(files), "folders in", LANDING_PATH)
display(dbutils.fs.ls(files[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC Peek at a raw file – this is exactly what a source system would hand you:

# COMMAND ----------

first_file = [f.path for f in dbutils.fs.ls(files[0]) if f.name.endswith(".json")][0]
print(dbutils.fs.head(first_file, 600))

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** chapter 5 ingests these files incrementally. Keep this notebook handy – you'll come back to run batch **2**.
