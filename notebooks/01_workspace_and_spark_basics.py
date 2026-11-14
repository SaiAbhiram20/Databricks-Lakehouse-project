# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Workspace tour and Spark basics
# MAGIC
# MAGIC **Goal:** get comfortable with notebooks, compute, and the DataFrame/SQL duality that everything else builds on.
# MAGIC
# MAGIC ### Workspace map (left sidebar)
# MAGIC | Item | What it is |
# MAGIC |---|---|
# MAGIC | **Workspace** | Your folders, notebooks, Git folders |
# MAGIC | **Catalog** | Browse catalogs → schemas → tables/volumes (Unity Catalog) |
# MAGIC | **Compute** | Clusters / serverless. On Free Edition you use *serverless* – nothing to create |
# MAGIC | **Jobs & Pipelines** | Scheduled jobs and Lakeflow pipelines |
# MAGIC | **SQL Editor / Dashboards / Genie** | Analyst-facing tools running on a SQL warehouse |
# MAGIC
# MAGIC ### Notebook rules of thumb
# MAGIC * A notebook has a **default language** (Python here). Switch per cell with `%sql`, `%md`, `%python`.
# MAGIC * `spark` (the SparkSession), `dbutils` and `display()` already exist – no setup.
# MAGIC * Run a cell with **Shift+Enter**. Cells share state in order, so run top to bottom.

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

# MAGIC %md
# MAGIC ## 1. Look at the data
# MAGIC `samples.nyctaxi.trips` is a table Databricks ships in every workspace.

# COMMAND ----------

trips = spark.table(SOURCE_TABLE)
trips.printSchema()
print("rows:", trips.count())
display(trips.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The same question in Python and in SQL
# MAGIC DataFrame code and SQL compile to the **same** execution plan. Use whichever reads better.
# MAGIC Question: *average fare and trip count per pickup ZIP, busiest first.*

# COMMAND ----------

by_zip = (
    trips.groupBy("pickup_zip")
    .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
    .orderBy(F.desc("trips"))
)
display(by_zip.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_zip, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
# MAGIC FROM samples.nyctaxi.trips
# MAGIC GROUP BY pickup_zip
# MAGIC ORDER BY trips DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core transformations
# MAGIC `select`, `filter`, `withColumn`, `groupBy`, `join` cover ~90% of day-to-day work.
# MAGIC Spark is **lazy**: transformations only build a plan; an *action* (`display`, `count`, `write`) runs it.

# COMMAND ----------

enriched = (
    trips
    .withColumn("duration_min",
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0))
    .select("pickup_hour", "trip_distance", "fare_amount", "duration_min")
)
enriched.explain()          # read the plan – nothing has executed yet
display(enriched.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Temp views: bridge Python ↔ SQL

# COMMAND ----------

enriched.createOrReplaceTempView("trips_enriched")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour, COUNT(*) AS trips, ROUND(AVG(fare_amount / trip_distance), 2) AS fare_per_mile
# MAGIC FROM trips_enriched
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY pickup_hour

# COMMAND ----------

# MAGIC %md
# MAGIC Click **+** next to the result table → *Visualization* to turn it into a chart.
# MAGIC
# MAGIC ## 5. dbutils and widgets

# COMMAND ----------

dbutils.fs.help()                       # file utilities (ls, cp, rm …)
dbutils.widgets.text("min_fare", "5", "Minimum fare")
min_fare = float(dbutils.widgets.get("min_fare"))
print("rows with fare >=", min_fare, ":", trips.filter(F.col("fare_amount") >= min_fare).count())

# COMMAND ----------

# MAGIC %md
# MAGIC Widgets appear at the top of the notebook and are how **jobs pass parameters** to notebooks later (chapter 11).
# MAGIC
# MAGIC ## Exercises
# MAGIC 1. What share of trips have `trip_distance = 0`? (DataFrame *and* SQL)
# MAGIC 2. Which pickup hour has the highest average `trip_distance`?
# MAGIC 3. Find the 5 most common (pickup_zip, dropoff_zip) pairs.
# MAGIC
# MAGIC <details><summary>Hints</summary>
# MAGIC
# MAGIC 1. `F.avg((F.col("trip_distance") == 0).cast("int"))`
# MAGIC 2. group by `pickup_hour` from the enriched DataFrame
# MAGIC 3. `groupBy("pickup_zip","dropoff_zip").count().orderBy(F.desc("count"))`
# MAGIC </details>
