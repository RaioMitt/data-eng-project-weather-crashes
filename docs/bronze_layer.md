# Bronze layer

The bronze layer holds raw data exactly as it comes from the source APIs. It lives in the `raw` schema of the weather DB. Cleaning and reshaping happen later, in the silver and gold layers.

## Weather data — `raw.weather_data_hourly`

Hourly weather measurements from the [keskkonnaandmed API](https://keskkonnaandmed.envir.ee) (`f_kliima_tund` endpoint), for all weather stations in Estonia.

Defined in [`src/schema/bronze.sql`](../src/schema/bronze.sql) and loaded by the Airflow DAG `fetch_weather_data_pipeline` ([`src/dags/fetch_weather_data.py`](../src/dags/fetch_weather_data.py)).

### Running the DAG

The DAG has no schedule, so you start it by hand:

- **Airflow UI**: open http://localhost:8080, find `fetch_weather_data_pipeline` and click ▶ (Trigger DAG).
- **Command line** (from `src/`):
  ```bash
  docker compose exec airflow-scheduler airflow dags trigger fetch_weather_data_pipeline
  ```

### Columns

The data is in long format: each row holds **one measurement** (one element) for one station in one hour.

| Column | Type | Description |
|---|---|---|
| `jaam_kood` | varchar(10) | Station code, e.g. `AJHARK01`. Use it to look up the station's coordinates. |
| `jaam_nimi` | varchar(100) | Station name, e.g. `Tallinn-Harku` |
| `aasta` | integer | Year (UTC) |
| `kuu` | integer | Month (UTC) |
| `paev` | integer | Day (UTC) |
| `tund` | integer | Hour, 0–23 (UTC) |
| `element_kood` | varchar(10) | Which measurement this row holds (see the element codes below) |
| `vaartus` | double precision | Measured value. Can be `NULL` if the station has no measurement for that hour. |
| `loaded_at` | timestamptz | When the row was first inserted into the DB |

**Primary key:** `(jaam_kood, aasta, kuu, paev, tund, element_kood)`

All times are in **UTC**. Crash times are probably in Estonian local time, so convert one or the other before joining.

### Element codes

The DAG fetches these elements (`DEFAULT_ELEMENTS` in the DAG):

| Code | Meaning | Unit |
|---|---|---|
| `TA` | Air temperature at the end of the hour | °C |
| `TAX1H` | Maximum air temperature during the hour | °C |
| `TAN1H` | Minimum air temperature during the hour | °C |
| `PR1H` | Precipitation during the hour | mm |
| `WSX1H` | Strongest wind gust during the hour | m/s |
| `SDUR1H` | Minutes of sunshine during the hour (0–60) | min |

The API also offers `WS10M`, `WD10M`, `RH` and `PA0` (listed in `POSSBIBLE_ELEMENTS` in the DAG). To fetch them as well, add them to `DEFAULT_ELEMENTS`.

### How the DAG loads data

1. It finds the latest month already in the table (highest `aasta`, then `kuu`).
2. **Empty table:** it loads every month from January 2025 up to and including the current month.
3. **Table has data:** it fetches again from that latest month up to and including the current month. The latest month is fetched again because it was probably only partly loaded last time.
4. Rows are **upserted**: a row whose key already exists gets its `jaam_nimi` and `vaartus` overwritten, and new rows are inserted. Running the DAG again, or the API returning duplicate rows, therefore never creates duplicates or errors.

The API returns at most 20 000 rows per request, so each month is fetched in pages.

On an upsert only `jaam_nimi` and `vaartus` are updated. `loaded_at` keeps the time of the first insert.
