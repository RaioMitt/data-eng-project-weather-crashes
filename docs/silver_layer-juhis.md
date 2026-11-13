# Silver layer — how to run

Silver is built from the bronze (`raw`) tables by the DAG `build_silver_pipeline` ([`src/schema/silver.sql`](../src/schema/silver.sql)). Each run drops and rebuilds the tables, so it's safe to run again.

## 1. Start

```bash
git pull origin main
cd src
docker compose up -d
```

If your `pgdata_weather/` is old and a bronze table is missing, run this (safe, only creates what's missing):

```bash
docker compose exec -T weather-db psql -f /docker-entrypoint-initdb.d/bronze.sql
```

## 2. Run the DAGs

Open http://localhost:8080 (`airflow` / `airflow`). Unpause all DAGs except `example_dbt_pipeline`.

1. Trigger ▶ `fetch_station_data_pipeline`, `fetch_weather_data_pipeline` and `ingest_accident_data_pipeline`. Wait until all are dark green. The first weather load takes a long time (it fetches everything since 2025-01).
2. Trigger ▶ `build_silver_pipeline`.

## 3. Check the result

Open http://localhost:5051 (`admin@example.com` / `admin`) → **Add New Server**:
host `weather-db`, port `5432`, database `weather_db`, user `weather_user`, password `weather_pass`.

Then **weather_db → Schemas → silver → Tables** → right-click a table → **View/Edit Data**.

## If something goes wrong

- **Port 8080 already allocated:** another container uses it. Stop it (`docker ps` shows which), then `docker compose down && docker compose up -d`.
- **No space left on device:** free up disk space, run `docker system prune`, restart Docker Desktop.
- **A DAG is red:** click the red square → **Logs**.
