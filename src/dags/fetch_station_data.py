"""fetch_station_data.py — Airflow DAG that loads weather station metadata
(code, name, coordinates, elevation, active period) into the bronze layer.

Source: keskkonnaandmed API, endpoint f_kliima_jaam_vaatlus
Target: raw.weather_stations (one JSONB document per station period)

The API returns the same station row many times (the sample has 2140 rows but
only 34 distinct ones), so rows are de-duplicated by
(jaam_kood, jaam_periood_algus) before loading. A station that has moved has
several rows with different periods and coordinates; all of them are kept.

Schedule: manual trigger only (station list rarely changes). Run it from the
Airflow UI or:

    docker compose exec airflow-scheduler \
        airflow dags trigger fetch_station_data_pipeline
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "weather_db"
BASE = "https://keskkonnaandmed.envir.ee"
ENDPOINT = "f_kliima_jaam_vaatlus"
TARGET_TABLE = "raw.weather_stations"

API_PAGE = 1_000   # rows per API request (server maximum is 20 000)
DB_BATCH = 500     # rows per INSERT statement

SELECT_COLUMNS = (
    "jaam_kood,jaam_nimi,jaam_nimi_eng,pikkuskraad,laiuskraad,"
    "korgus_merepinnast_m,jaam_periood_algus,jaam_periood_lopp"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
}


def get(table: str, params: dict, retries: int = 3):
    """GET a PostgREST endpoint and return the parsed JSON (with retries)."""
    url = f"{BASE}/{table}?{urllib.parse.urlencode(params, safe='(),.*')}"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return json.load(r)
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"  retrying after error: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)


def fetch_stations() -> list[dict]:
    """Fetch all station rows from the API, page by page."""
    params = {
        "select": SELECT_COLUMNS,
        # A stable order is needed for limit/offset paging. Rows that tie on
        # this order are identical copies, so paging can't lose a distinct row.
        "order": "jaam_kood,jaam_periood_algus",
    }
    rows, offset = [], 0
    while True:
        batch = get(ENDPOINT, {**params, "limit": API_PAGE, "offset": offset})
        print(f"Fetched {len(batch)} rows (offset {offset})")
        rows.extend(batch)
        if len(batch) < API_PAGE:
            return rows
        offset += API_PAGE


def deduplicate(rows: list[dict]) -> list[dict]:
    """Keep one row per (jaam_kood, jaam_periood_algus)."""
    unique = {}
    for r in rows:
        unique[(r["jaam_kood"], r["jaam_periood_algus"])] = r
    return list(unique.values())


def upsert_stations(rows: list[dict]) -> None:
    """Insert rows as JSONB; update a row only if its content changed."""
    from psycopg2.extras import Json, execute_values

    sql = f"""
        INSERT INTO {TARGET_TABLE} (data)
        VALUES %s
        ON CONFLICT (jaam_kood, jaam_periood_algus) DO UPDATE
            SET data = EXCLUDED.data,
                updated_at = now()
            WHERE {TARGET_TABLE}.data IS DISTINCT FROM EXCLUDED.data
    """
    pg = PostgresHook(postgres_conn_id=CONN_ID)
    with pg.get_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur, sql, [(Json(r),) for r in rows],
            template="(%s::jsonb)", page_size=DB_BATCH,
        )
    # leaving the `with conn` block commits the transaction


def ingest_station_data():
    rows = fetch_stations()
    if not rows:
        raise ValueError(f"{ENDPOINT} returned no rows, nothing loaded")
    unique = deduplicate(rows)
    print(f"{len(rows)} rows from API, {len(unique)} distinct station periods, "
          f"{len({r['jaam_kood'] for r in unique})} stations")
    upsert_stations(unique)
    print(f"Upserted {len(unique)} rows into {TARGET_TABLE}")


with DAG(
    dag_id="fetch_station_data_pipeline",
    description="fetch weather station metadata into the bronze layer",
    schedule=None,                          # trigger manually
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "weather", "stations"],
) as dag:
    t_ingest = PythonOperator(
        task_id="ingest_station_data",
        python_callable=ingest_station_data,
        retries=3,
    )
