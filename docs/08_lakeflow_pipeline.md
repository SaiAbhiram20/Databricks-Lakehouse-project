# 08 · Lakeflow Declarative Pipelines

*(Formerly Delta Live Tables / DLT. If your menus say "Delta Live Tables", it's the same thing.)*

In chapters 5–7 **you** wrote the orchestration: run bronze, then silver, then gold; manage checkpoints; handle failures.
A declarative pipeline flips this. You define each table with a `SELECT`; Databricks builds the dependency graph,
runs it in the right order, manages checkpoints, retries, and tracks data-quality **expectations**.

| Hand-built (ch. 5–7) | Declarative (this chapter) |
|---|---|
| `readStream` + `writeStream` + checkpoint | `CREATE STREAMING TABLE … AS SELECT … FROM STREAM …` |
| `if/else` filters + quarantine table | `CONSTRAINT … EXPECT (…) ON VIOLATION DROP ROW` |
| `CREATE OR REPLACE TABLE` for gold | `CREATE MATERIALIZED VIEW` (incremental refresh) |
| You schedule/order the notebooks | Pipeline resolves the DAG automatically |

## Steps

1. Make sure batch 1 (and optionally 2) is in the volume (chapter 4).
2. Open [`pipeline/taxi_pipeline.sql`](../pipeline/taxi_pipeline.sql). If you changed `CATALOG`/`SCHEMA`, edit the
   landing path inside `read_files(...)`.
3. In Databricks: **Jobs & Pipelines → Create → ETL pipeline**.
   * Name: `taxi_pipeline`
   * Default catalog / schema: the same `CATALOG` / `learn_databricks`
   * Add source code: import `taxi_pipeline.sql` into your workspace first (Workspace → Import), then pick it.
   * Compute: serverless.
4. Click **Run pipeline** (use *Development* mode while learning so failures are quick to retry).
5. Explore the **graph view**: click a table to see rows written, **expectations** (passed / dropped), and the schema.
6. Land batch 2 (notebook 04, widget = 2) and run the pipeline again. Only new files are processed.

## Things to check

* Row counts in `bronze_trips_lf` vs `silver_trips_lf`: the difference is dropped rows. How does it compare with
  `silver_quarantine` from chapter 6?
* Expectation metrics: click `silver_trips_lf` → *Data quality* tab.
* The pipeline **event log** is queryable (use the pipeline's *Settings → Event log* location, or
  `SELECT * FROM event_log(TABLE(silver_trips_lf))`).

## What the pipeline does *not* do (yet)

It doesn't de-duplicate. In a streaming pipeline this is done with `AUTO CDC INTO` (formerly `APPLY CHANGES INTO`),
keyed on `trip_id`. **Stretch exercise:** replace the silver streaming table with a `CREATE FLOW … AS AUTO CDC INTO`
and compare the row counts with chapter 6. (Check the current docs for exact syntax — it has changed across releases.)

## Exercises

1. Add an expectation `fare_amount < 500` as a *warning* and see how many rows it flags.
2. Add `gold_hourly_demand_lf` as a second materialized view.
3. Change an expectation to `ON VIOLATION FAIL UPDATE`. When would you prefer that over `DROP ROW`?
