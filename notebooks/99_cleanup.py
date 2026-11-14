# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Clean up
# MAGIC Removes **everything this guide created** by dropping its schema (with `CASCADE`). The schema is
# MAGIC `CATALOG.SCHEMA` from `00_config` – read the printed name before confirming.
# MAGIC
# MAGIC Also delete by hand (UI): the `taxi_pipeline` pipeline, the `taxi_daily` job, the dashboard and any Genie space.
# MAGIC Dropping tables that a running pipeline owns will fail until the pipeline is deleted first.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Really delete?")
target = f"{CATALOG}.{SCHEMA}"
print(f"This will permanently drop schema: {target}  (tables, volume files, checkpoints)")

if dbutils.widgets.get("confirm") == "yes":
    spark.sql(f"DROP SCHEMA IF EXISTS {target} CASCADE")
    print("Dropped", target)
else:
    print("Nothing deleted. Set the widget to 'yes' and re-run this cell to delete.")
