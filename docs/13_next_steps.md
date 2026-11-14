# 13 · Next steps

You now have working knowledge of the core data engineering path on Databricks. This chapter points to what to learn
next, in rough priority order for a data engineer.

## 1. Deploy as code with Databricks Asset Bundles (DABs)

Everything you clicked together (jobs, pipelines) can live in Git as YAML and be deployed by CI/CD. Skeleton
(**example only – check current docs for exact schema and CLI version**):

```yaml
# databricks.yml
bundle:
  name: taxi_lakehouse

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://<your-workspace>.cloud.databricks.com

resources:
  jobs:
    taxi_daily:
      name: taxi_daily
      tasks:
        - task_key: bronze
          notebook_task: { notebook_path: ./notebooks/05_bronze_autoloader.py }
        - task_key: silver
          depends_on: [{ task_key: bronze }]
          notebook_task: { notebook_path: ./notebooks/06_silver_cleaning.py }
```
Commands: `databricks bundle validate`, `databricks bundle deploy -t dev`, `databricks bundle run taxi_daily`.
Install the Databricks CLI first and authenticate (`databricks auth login`).

## 2. Performance fundamentals
* Read the **Spark UI** / query profile; learn to spot skew, spills and shuffles.
* Prefer **liquid clustering** over manual partitioning for new Delta tables; avoid over-partitioning small tables.
* Understand **Photon**, adaptive query execution, broadcast joins, file sizing, and `OPTIMIZE`.

## 3. Streaming deeper
Continuous triggers, watermarks, stateful aggregations, `foreachBatch`, Kafka/Event Hubs sources, exactly-once semantics.

## 4. Data modelling on the lakehouse
Dimensional modelling (SCD2 with MERGE / `AUTO CDC`), constraints (PK/FK informational), Delta Sharing for cross-org data.

## 5. ML and AI (optional branch)
MLflow tracking and model registry in UC, Feature Store, Model Serving, Vector Search, Agent Framework.

## 6. Certification
* **Databricks Certified Data Engineer Associate** — this guide covers chapters 1–12's ground; confirm the current exam
  guide on the Databricks site because topics change.
* Then **Data Engineer Professional**.

## 7. Portfolio project ideas
1. Replace the sample data with a real public API (ingest to a volume on a schedule).
2. Add an SCD2 dimension table (`dim_zip`) and a star-schema gold layer.
3. Add CI: bundle validate + a PySpark unit-test job on every pull request.
4. Add streaming: a file-arrival-triggered job that processes each new file within minutes.

## Self-check (can you explain these without notes?)
* What is the difference between a managed table, an external table and a volume?
* What does the Delta transaction log give you? How does time travel work, and what limits it?
* Why is Auto Loader's checkpoint important? What happens if you delete it?
* Why quarantine instead of drop? Why MERGE instead of append?
* Declarative pipeline vs hand-written notebooks: trade-offs?
* How would you let an analyst see a table but not the ZIP column?
