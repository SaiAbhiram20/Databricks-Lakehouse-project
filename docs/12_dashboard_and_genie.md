# 12 · AI/BI Dashboard and Genie

*(Dashboards were formerly "Lakeview".)* Gold tables exist to be consumed. Build a dashboard on them.

## Create the dashboard

1. **Dashboards → Create dashboard**. Name: `NYC Taxi Overview`.
2. **Data** tab → **Create from SQL** (one dataset per query). Paste each query below (adjust catalog/schema):

**Dataset `daily`**
```sql
SELECT pickup_date, trips, revenue, avg_fare
FROM workspace.learn_databricks.gold_daily_metrics
ORDER BY pickup_date
```

**Dataset `hourly`**
```sql
SELECT pickup_hour, trips, avg_fare
FROM workspace.learn_databricks.gold_hourly_demand
ORDER BY pickup_hour
```

**Dataset `routes`**
```sql
SELECT CONCAT(pickup_zip, ' → ', dropoff_zip) AS route, trips, avg_fare
FROM workspace.learn_databricks.gold_top_routes
ORDER BY trips DESC
LIMIT 15
```

**Dataset `kpis`**
```sql
SELECT SUM(trips) AS total_trips, ROUND(SUM(revenue), 0) AS total_revenue, ROUND(SUM(revenue) / SUM(trips), 2) AS avg_fare
FROM workspace.learn_databricks.gold_daily_metrics
```

3. **Canvas** tab → add visualizations:
   * Counters from `kpis` (total trips, revenue, average fare)
   * Line chart: `daily` → X `pickup_date`, Y `trips`
   * Bar chart: `hourly` → X `pickup_hour`, Y `trips`
   * Table or bar chart: `routes`
4. Add a **date range filter** on `pickup_date` (Filter widget bound to `daily`).
5. **Publish**. Choose whether viewers use *your* credentials (embedded) or their own (UC permissions apply).
6. Optional: **Schedule** a refresh/subscription so stakeholders get it by email.

## Genie: ask questions in plain English

1. **Genie → New**, add the three gold tables as data.
2. Add instructions: "A *trip* is one row in gold tables; revenue is the sum of fare_amount."
3. Ask: *"Which hour has the most trips?"*, *"Show daily revenue as a line chart"*, *"What is the busiest route?"*
4. Open the generated SQL for each answer and **check it**. Genie is a productivity tool, not a source of truth.
   Good table/column comments (chapter 2, 9) directly improve its answers.

## Design checklist

* One question per chart; title says the takeaway.
* Dashboard reads **gold only**.
* Sensible defaults on filters; no raw IDs in tables.
* Refresh schedule matches the pipeline schedule (chapter 11).
