-- Lakeflow Declarative Pipelines source (formerly Delta Live Tables).
-- The same bronze -> silver -> gold flow as notebooks 05-07, but DECLARATIVE:
-- you say WHAT each table is; Databricks works out ordering, checkpoints, retries and incremental refresh.
--
-- BEFORE USE: if you changed CATALOG/SCHEMA in 00_config, edit the landing path below.
-- Tables are created in the catalog/schema you choose in the pipeline settings (suffix _lf avoids clashing
-- with the tables from chapters 5-7).

-- ---------- BRONZE: incremental ingest (like Auto Loader) ----------
CREATE OR REFRESH STREAMING TABLE bronze_trips_lf
COMMENT 'Raw taxi trip JSON, ingested incrementally'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/learn_databricks/landing/trips',
  format => 'json'
);

-- ---------- SILVER: typed + validated with EXPECTATIONS ----------
-- ON VIOLATION DROP ROW  -> row is removed, and the count of drops is recorded in the pipeline event log
-- (no ON VIOLATION)       -> row is kept, violation is only counted (warn)
CREATE OR REFRESH STREAMING TABLE silver_trips_lf (
  CONSTRAINT has_timestamps      EXPECT (pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_fare       EXPECT (fare_amount > 0)                                  ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_dist   EXPECT (trip_distance >= 0)                               ON VIOLATION DROP ROW,
  CONSTRAINT dropoff_after_pick  EXPECT (dropoff_ts >= pickup_ts)                          ON VIOLATION DROP ROW,
  CONSTRAINT plausible_distance  EXPECT (trip_distance < 100)
)
COMMENT 'Typed, validated trips'
AS SELECT
  trip_id,
  CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
  CAST(trip_distance AS DOUBLE)            AS trip_distance,
  CAST(fare_amount   AS DOUBLE)            AS fare_amount,
  CAST(pickup_zip    AS INT)               AS pickup_zip,
  CAST(dropoff_zip   AS INT)               AS dropoff_zip,
  CAST(pickup_date   AS DATE)              AS pickup_date
FROM STREAM bronze_trips_lf;

-- ---------- GOLD: materialized views refresh incrementally when possible ----------
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_metrics_lf
COMMENT 'Daily trips and revenue'
AS SELECT
  pickup_date,
  COUNT(*)                       AS trips,
  ROUND(SUM(fare_amount), 2)     AS revenue,
  ROUND(AVG(fare_amount), 2)     AS avg_fare
FROM silver_trips_lf
GROUP BY pickup_date;
