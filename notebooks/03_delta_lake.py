# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Delta Lake: reliable tables on files
# MAGIC
# MAGIC Delta Lake = Parquet data files + a **transaction log** (`_delta_log`). The log gives you:
# MAGIC ACID transactions, schema enforcement, `UPDATE/DELETE/MERGE`, **time travel**, and concurrent-safe writes.
# MAGIC Every table you create on Databricks is Delta by default.
# MAGIC
# MAGIC **Goal:** do real DML on a table, read its history, travel back in time, and see schema enforcement block bad data.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a practice table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS delta_demo")
(
    spark.table(SOURCE_TABLE).limit(1000)
    .withColumn("trip_id", F.monotonically_increasing_id())
    .write.format("delta").saveAsTable("delta_demo")
)
display(spark.sql("SELECT COUNT(*) AS n FROM delta_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Change data (each statement is one atomic commit)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta_demo SET fare_amount = fare_amount * 1.10 WHERE trip_distance > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta_demo WHERE trip_distance = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. History = audit log

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC Each row is a version: `operation`, `operationMetrics` (rows affected), who and when.
# MAGIC
# MAGIC ## 4. Time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'v0 (original)' AS version, COUNT(*) AS n FROM delta_demo VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'now', COUNT(*) FROM delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- undo the delete/update by restoring the original version
# MAGIC RESTORE TABLE delta_demo TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS n_after_restore FROM delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MERGE: the upsert you will use constantly
# MAGIC Update rows that match, insert rows that don't – in one transaction.

# COMMAND ----------

changes = spark.table("delta_demo").limit(5).withColumn("fare_amount", F.col("fare_amount") + 100)
brand_new = spark.table("delta_demo").limit(2).withColumn("trip_id", F.col("trip_id") + F.lit(10**12))
changes.unionByName(brand_new).write.mode("overwrite").saveAsTable("delta_changes")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_demo AS t
# MAGIC USING delta_changes AS s
# MAGIC ON t.trip_id = s.trip_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_now FROM delta_demo   -- 2 more than before: 5 updated, 2 inserted

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema enforcement and evolution
# MAGIC Delta **rejects** writes that don't match the table schema – bad data can't sneak in.

# COMMAND ----------

extra = spark.table("delta_demo").limit(3).withColumn("tip_amount", F.lit(1.5))
try:
    extra.write.mode("append").saveAsTable("delta_demo")
except Exception as e:
    print("Blocked as expected:", type(e).__name__)

# COMMAND ----------

# Opt in to evolving the schema deliberately:
extra.write.mode("append").option("mergeSchema", "true").saveAsTable("delta_demo")
display(spark.sql("DESCRIBE delta_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Maintenance: OPTIMIZE and VACUUM
# MAGIC * `OPTIMIZE` compacts many small files into fewer big ones (faster reads).
# MAGIC * `VACUUM` physically deletes files no longer referenced and older than the retention period (default 7 days).
# MAGIC   Time travel beyond that window stops working – so don't shorten retention casually.
# MAGIC * On managed tables, Databricks can run these for you (*predictive optimization*).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC 1. Use `DESCRIBE HISTORY` to find the version number right after your MERGE, then query the table at that version.
# MAGIC 2. Write a MERGE that **deletes** matching rows (`WHEN MATCHED THEN DELETE`).
# MAGIC 3. Look at `DESCRIBE DETAIL delta_demo` – how many files? Run `OPTIMIZE` again – does the number change?
