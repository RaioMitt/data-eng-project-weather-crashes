# Gold layer — how to run

Gold is the star schema, built with dbt from the silver models by the DAG `dbt_gold_pipeline`
([`src/dbt_project/models/gold/`](../src/dbt_project/models/gold/)). Each run drops and
rebuilds the gold tables, so it's safe to run again.

## 1. Start

```bash
git pull origin main
cd src
docker compose up -d
```

## 2. Run the DAGs

Gold is built from silver, so the earlier layers must be built first.

Open http://localhost:8080 (`airflow` / `airflow`). Unpause all DAGs.

1. Trigger ▶ `fetch_station_data_pipeline`, `fetch_weather_data_pipeline` and `ingest_accident_data_pipeline`. Wait until all are dark green.
2. Trigger ▶ `dbt_silver_pipeline` and wait until it is dark green.
3. Trigger ▶ `dbt_gold_pipeline`. It has two tasks, `dbt_run_gold` (builds the tables) and `dbt_test_gold` (runs the tests); both must be dark green.

Without Airflow, the same can be done from the `src` folder:

```bash
docker compose exec dbt dbt build --project-dir /dbt --profiles-dir /dbt --select tag:gold
```

## 3. Check the result

Open http://localhost:5051 (`admin@example.com` / `admin`) → weather_db → Schemas → gold → Tables.
There should be six tables: `crash_type_dimension`, `crashes_fact_table`, `location_dimension`,
`time_dimension`, `weather_fact_table` and `weather_station_dimension`.

Right-click weather_db → Query Tool and run:

```sql
SELECT (SELECT COUNT(*) FROM gold.crashes_fact_table) AS crashes,
       (SELECT COUNT(*) FROM silver.crashes)          AS silver_crashes,
       (SELECT COUNT(*) FROM gold.weather_fact_table) AS weather,
       (SELECT COUNT(*) FROM silver.weather_hourly)   AS silver_weather;
```

Each gold count must equal its silver count. Crashes with the weather of their hour:

```sql
SELECT c.id, c.damage_amount, c.personal_injury, t.date, t.hour AS local_hour,
       w.max_temperature, w.precipitation, w.max_wind_speed
FROM gold.crashes_fact_table c
JOIN gold.time_dimension t ON t.id = c.time_id
JOIN gold.weather_fact_table w
     ON w.time_id = c.time_id AND w.station_id = c.station_id
LIMIT 10;
```

## If something goes wrong

* `dbt_gold_pipeline` is red: click the red square → Logs. A failed task `dbt_test_gold` means a data check failed; the log names the test.
* `relation "silver...." does not exist`: run `dbt_silver_pipeline` first.
* A gold table appears in a schema called `public_gold` instead of `gold`: the macro `generate_schema_name` in `src/dbt_project/macros` must accept `gold`.
* The DAG is missing from the list: wait a few minutes and refresh; an "Import Errors" banner at the top of the page names the problem.