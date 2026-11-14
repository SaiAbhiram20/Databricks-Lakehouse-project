# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Unity Catalog: where everything lives
# MAGIC
# MAGIC Unity Catalog (UC) is Databricks' governance layer. Every data object has a three-level name:
# MAGIC
# MAGIC ```
# MAGIC catalog . schema . table      e.g. workspace.learn_databricks.silver_trips
# MAGIC                  . volume     (files)   /Volumes/<catalog>/<schema>/<volume>/...
# MAGIC                  . function / model
# MAGIC ```
# MAGIC
# MAGIC * **Managed table** – UC owns the storage and lifecycle (default; dropping the table deletes data).
# MAGIC * **External table** – you own the files in a cloud location UC points at.
# MAGIC * **Volume** – governed storage for *non-tabular* files (JSON, CSV, images, checkpoints).
# MAGIC
# MAGIC **Goal:** explore the hierarchy, create objects, and know where to look things up.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS me

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create your first managed table
# MAGIC A *CTAS* (`CREATE TABLE AS SELECT`) copies data into UC-managed Delta storage.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_demo_trips
# MAGIC COMMENT 'Practice copy of the NYC taxi sample'
# MAGIC AS SELECT * FROM samples.nyctaxi.trips LIMIT 5000

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED uc_demo_trips

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `Type: MANAGED`, `Provider: delta`, and the storage `Location`. Now open **Catalog** in the sidebar and
# MAGIC navigate `workspace → learn_databricks → uc_demo_trips` – the *Sample Data*, *Details*, *Permissions* and
# MAGIC *Lineage* tabs are all there.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumes: governed file storage

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES

# COMMAND ----------

dbutils.fs.put(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/hello.txt", "hello unity catalog", overwrite=True)
display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Volume paths work like normal file paths in Spark **and** plain Python:

# COMMAND ----------

with open(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/hello.txt") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover metadata with `information_schema`
# MAGIC Every catalog exposes SQL-queryable metadata – great for audits and automation.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, table_type, comment
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = '{SCHEMA}'
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE uc_demo_trips IS 'Practice copy – safe to drop';
# MAGIC -- SHOW GRANTS ON TABLE uc_demo_trips   -- who can do what (chapter 9)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC 1. Create a second schema `scratch`, a table in it, and query it with its full three-level name.
# MAGIC 2. Query `information_schema.columns` to list the columns and types of `uc_demo_trips`.
# MAGIC 3. Drop `uc_demo_trips`. What happens to its data (managed table)? Then try `UNDROP TABLE` (works within a retention window).
