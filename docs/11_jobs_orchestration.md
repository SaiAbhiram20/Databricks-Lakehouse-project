# 11 · Orchestration with Lakeflow Jobs

*(Formerly Databricks Workflows.)* A **Job** runs tasks on a schedule or trigger, in dependency order, with retries
and notifications. Tasks can be notebooks, SQL, pipelines, Python scripts, dbt, other jobs and more.

## Goal: run the whole medallion flow daily

```
05_bronze_autoloader ──► 06_silver_cleaning ──► 07_gold_aggregates ──► 10_data_quality (fail_on_critical = true)
```

## Build it in the UI

1. **Jobs & Pipelines → Create → Job**. Name it `taxi_daily`.
2. **Add task** 1:
   * Task name `bronze` · Type **Notebook** · Source *Workspace* · Path → your `05_bronze_autoloader`
   * Compute: **Serverless**
3. **Add task** 2 `silver` → notebook `06_silver_cleaning`, **Depends on** `bronze`.
4. **Add task** 3 `gold` → notebook `07_gold_aggregates`, depends on `silver`.
5. **Add task** 4 `quality` → notebook `10_data_quality`, depends on `gold`, **Parameters**: key `fail_on_critical`, value `true`.
6. Job-level settings:
   * **Schedule**: e.g. daily 06:00 in your time zone (cron `0 0 6 * * ?`).
   * **Retries** on each task: 1–2 retries, short delay (handles transient failures, not logic bugs).
   * **Notifications**: email on failure.
   * **Max concurrent runs**: 1 (a second run must not overlap the first).
7. **Run now**, then open the **Runs** tab: timeline, per-task output, duration.

## Concepts to internalise

| Concept | Why it matters |
|---|---|
| **Task dependencies (DAG)** | Silver never starts if bronze fails |
| **Parameters** | Same notebook, different behaviour. Notebooks read them with `dbutils.widgets.get(...)` |
| **Idempotency** | Retries and re-runs must be safe. Our MERGE + checkpoints make them so |
| **Job cluster vs serverless** | Serverless starts fast and needs no sizing; classic job clusters give control and can be cheaper at scale |
| **Repair run** | Re-run only the failed task and downstream tasks |
| **Triggers** | Time schedule, *file arrival* on a volume, table update, or continuous |
| **Service principals** | In production run jobs as a service principal, not your personal user |

## Experiments

1. Set `fail_on_critical=true`, insert a bad row into silver (see notebook 10), run the job → `quality` fails, email fires.
   Fix the data and use **Repair run**.
2. Change the trigger from time-based to **file arrival** on `/Volumes/.../landing/trips/` and land batch 2 (notebook 04).
3. Add a task for the Lakeflow pipeline (type *Pipeline*) in place of tasks 1–3 and compare the run graphs.
4. Open the job's **⋮ → View YAML / JSON** — this is the definition you'll later keep in Git (see chapter 13).

> Free Edition caveats: limits on concurrent runs and daily compute apply; schedules may be restricted. If a feature is
> missing, just run the job manually and treat the schedule steps as reading material.
