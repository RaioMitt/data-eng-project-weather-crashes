# Gold layer — how to run

Gold is the star schema built from the silver tables by the DAG `build_gold_pipeline`
(`src/dags/build_gold.py`, SQL in `src/schema/gold.sql`). Each run drops and rebuilds
the six gold tables, so it's safe to run again.

## 1. Start

Silver must be built first, because gold reads from it. If you haven't, follow
`silver_layer-juhis.md` up to and including `build_silver_pipeline`.

```bash
git pull origin main
cd src
docker compose up -d
```

## 2. Run the DAG

1. Open http://localhost:8080 (`airflow` / `airflow`).
2. Unpause `build_gold_pipeline`.
3. Trigger ▶ `build_gold_pipeline` and wait until it is dark green.

Run it again whenever silver has been rebuilt, so gold picks up the new data.

## 3. Check the result

Open http://localhost:5051 → weather_db → Schemas → gold → Tables. Or run:

```sql
SELECT (SELECT COUNT(*) FROM gold.time_dimension)            AS time_rows,
       (SELECT COUNT(*) FROM gold.location_dimension)        AS locations,
       (SELECT COUNT(*) FROM gold.crash_type_dimension)      AS crash_types,
       (SELECT COUNT(*) FROM gold.weather_station_dimension) AS stations,
       (SELECT COUNT(*) FROM gold.crashes_fact_table)        AS crashes,
       (SELECT COUNT(*) FROM gold.weather_fact_table)        AS weather;
```

`crashes` should equal `silver.crashes` and `weather` should equal `silver.weather_hourly`.
Example counts at the time of writing: 16,344 time rows, 1,505 locations, 32 crash types,
34 stations, 30,009 crashes.

Crashes joined to their weather (same hour, nearest station):

```sql
SELECT c.id, c.damage_amount, c.personal_injury, t.date, t.hour,
       w.max_temperature, w.precipitation, w.max_wind_speed
FROM gold.crashes_fact_table c
JOIN gold.time_dimension t ON t.id = c.time_id
JOIN gold.weather_fact_table w
     ON w.time_id = c.time_id AND w.station_id = c.station_id
LIMIT 10;
```

## What gold.sql does

| Table | Contents |
|---|---|
| `time_dimension` | One row per hour, from the earliest to the latest date in crashes or weather |
| `location_dimension` | Distinct county / municipality / settlement from crashes |
| `crash_type_dimension` | Distinct crash types |
| `weather_station_dimension` | Copied from `silver.weather_stations` |
| `crashes_fact_table` | One row per crash, with ids for time, location, crash type and weather station |
| `weather_fact_table` | One row per station and hour, with ids for time and station |

**Station assignment.** Silver crashes have no station, so gold gives each crash the nearest
station that was active on the crash date. Silver crash coordinates are in meters (L-EST97)
while station coordinates are longitude/latitude, so the function `gold.lest97(lon, lat)`
converts station coordinates to L-EST97 meters before distances are compared. PostGIS is not
available in our database image, so the conversion is plain SQL.

## Differences from the original schema design

| Item | Original design | Now | Reason |
|---|---|---|---|
| `location_dimension.county` | VARCHAR(50) | VARCHAR(255) | Matches silver; a value exceeded 50 |
| `location_dimension.municipality`, `settlement` | VARCHAR(50) | VARCHAR(500) | Matches silver |
| `crash_type_dimension.crash_type` | VARCHAR(50) | VARCHAR(500) | Matches silver |
| `weather_station_dimension.station_code` | VARCHAR(20) | VARCHAR(50) | Matches silver |
| `weather_station_dimension.name` | VARCHAR(100) | VARCHAR(500) | Matches silver |
| `weather_station_dimension.x/y_coordinate` | INTEGER | NUMERIC(9,6) | Silver stores degrees; INTEGER rounded them to whole degrees |
| `weather_station_dimension` | `is_current` | `valid_from`, `valid_to` | Matches the data model diagram and silver |
| Foreign keys | Declared from dimension `id` to fact | Declared from fact to dimension `id` | The original direction is rejected by Postgres |
| Table names | `"Crashes_fact_table"` etc., default schema | `gold.crashes_fact_table` etc., lowercase, `gold` schema | No quoting needed; matches the silver convention |

## Known limitations

* About 960 crashes have no crash type, because the source value is empty. They keep
  `crashtype_id = NULL` in `crashes_fact_table`.

## If something goes wrong

* `relation "silver...." does not exist`: run `build_silver_pipeline` first.
* DAG not in the list: wait a few minutes and refresh; check for an Import Errors banner.
* A DAG is red: click the red square → Logs.
* Row counts differ from silver: don't use the data. Rerun silver, then gold, and check the logs.