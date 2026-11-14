# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Shared config
# MAGIC
# MAGIC Every other notebook starts with `%run ./00_config`. That runs this notebook first, so the variables below
# MAGIC (`CATALOG`, `SCHEMA`, paths…) are available everywhere and you only ever change them in **one** place.
# MAGIC
# MAGIC **Before you run it:** open a SQL cell and run `SHOW CATALOGS`. On Databricks **Free Edition** you will see
# MAGIC a catalog called `workspace` (used below). On a paid workspace pick a catalog you may write to and change
# MAGIC `CATALOG`.

# COMMAND ----------

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
