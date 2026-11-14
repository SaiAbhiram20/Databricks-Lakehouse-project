# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Gold: business-ready aggregates
# MAGIC
# MAGIC **Gold** tables answer business questions directly. They are small, named for the consumer, and rebuilt from silver.
# MAGIC Dashboards and analysts read gold – never bronze.

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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_metrics
# MAGIC COMMENT 'Daily trip volume and revenue'
# MAGIC AS
# MAGIC SELECT
# MAGIC   pickup_date,
# MAGIC   COUNT(*)                                   AS trips,
# MAGIC   ROUND(SUM(fare_amount), 2)                 AS revenue,
# MAGIC   ROUND(AVG(fare_amount), 2)                 AS avg_fare,
# MAGIC   ROUND(AVG(trip_distance), 2)               AS avg_distance_miles,
# MAGIC   ROUND(AVG((unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60), 1) AS avg_duration_min
# MAGIC FROM silver_trips
# MAGIC GROUP BY pickup_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_hourly_demand
# MAGIC COMMENT 'Trips and average fare by hour of day'
# MAGIC AS
# MAGIC SELECT
# MAGIC   HOUR(pickup_ts)             AS pickup_hour,
# MAGIC   COUNT(*)                    AS trips,
# MAGIC   ROUND(AVG(fare_amount), 2)  AS avg_fare
# MAGIC FROM silver_trips
# MAGIC GROUP BY HOUR(pickup_ts)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_top_routes
# MAGIC COMMENT 'Most frequent pickup→dropoff ZIP pairs'
# MAGIC AS
# MAGIC SELECT pickup_zip, dropoff_zip, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
# MAGIC FROM silver_trips
# MAGIC GROUP BY pickup_zip, dropoff_zip
# MAGIC ORDER BY trips DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_daily_metrics ORDER BY pickup_date

# COMMAND ----------

# MAGIC %md
# MAGIC Make a line chart: **+ → Visualization →** X = `pickup_date`, Y = `trips`. Then open
# MAGIC **Catalog → gold_daily_metrics → Lineage** to see bronze → silver → gold drawn automatically.
# MAGIC
# MAGIC ## Exercises
# MAGIC 1. Add `gold_weekday_profile` (average trips per day of week; use `date_format(pickup_date,'E')`).
# MAGIC 2. Which day had the highest revenue? Does it match the busiest day by trips?
# MAGIC 3. Why use `CREATE OR REPLACE TABLE` here rather than appending?
