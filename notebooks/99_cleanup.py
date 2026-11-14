# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Clean up
# MAGIC Removes **everything this guide created** by dropping its schema (with `CASCADE`). The schema is
# MAGIC `CATALOG.SCHEMA` from `00_config` – read the printed name before confirming.
# MAGIC
# MAGIC Also delete by hand (UI): the `taxi_pipeline` pipeline, the `taxi_daily` job, the dashboard and any Genie space.
# MAGIC Dropping tables that a running pipeline owns will fail until the pipeline is deleted first.

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

dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Really delete?")
target = f"{CATALOG}.{SCHEMA}"
print(f"This will permanently drop schema: {target}  (tables, volume files, checkpoints)")

if dbutils.widgets.get("confirm") == "yes":
    spark.sql(f"DROP SCHEMA IF EXISTS {target} CASCADE")
    print("Dropped", target)
else:
    print("Nothing deleted. Set the widget to 'yes' and re-run this cell to delete.")
