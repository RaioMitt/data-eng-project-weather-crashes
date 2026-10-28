
from __future__ import annotations

import pendulum
from airflow import DAG
import json
import sys
import time
import urllib.parse
import urllib.request
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
DBT_PROJECT_DIR = "/dbt"
DBT_PROFILES_DIR = "/dbt"
CONN_ID = "weather_db"

# Common flags added to every dbt subcommand.
# In dbt 1.8+, --project-dir and --profiles-dir are subcommand flags
# (they follow the subcommand, not dbt itself).
DBT_FLAGS = (
    f"--no-use-colors "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR}"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

BASE = "https://keskkonnaandmed.envir.ee"
PAGE = 20_000  # server-side maximum rows per request
POSSBIBLE_ELEMENTS = ["TA", "TAX1H", "TAN1H", "WS10M", "WSX1H", "WD10M", "PR1H", "RH", "PA0", "SDUR1H"]
DEFAULT_ELEMENTS = ["TA", "TAX1H", "TAN1H", "PR1H", "WSX1H", "SDUR1H"]
"""
TA: temp during end of the hour
TAX1H: hourly max
TAN1H: hourly min
PR1H: Precipitation during the hour	mm
WSX1H: Strongest wind gust during the hour
SDUR1H: Minutes of sunshine during the hour (0-60 min)
"""


def ingest_weather_data():
    pg = PostgresHook(postgres_conn_id=CONN_ID)
    rows = pg.get_records("SELECT kuu, aasta FROM raw.weather_data_hourly ORDER BY aasta DESC, kuu DESC LIMIT 1")
    #db has no historical data, so we need to ingest the whole dataset from 2025-01
    if len(rows) == 0:
        ingest_during_and_after_specified_month(1, 2025)
    else:
        # Determine the last month for which we have data
        last_month = max(row[0] for row in rows)
        last_year = max(row[1] for row in rows)
        #upsert from last month to current month
        ingest_during_and_after_specified_month(last_month, last_year)

def ingest_during_and_after_specified_month(month:int, year:int):
    pg = PostgresHook(postgres_conn_id=CONN_ID)
    current_month = datetime.now().month
    current_year = datetime.now().year
    while year < current_year or (year == current_year and month <= current_month):
        print(f"Fetching data for {year}-{month:02d}")
        rows = fetch_month(None, year, month)
        if rows:
            upsert_monthly_data(rows)
        else:
            print(f"No data for {year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
def upsert_monthly_data(rows):
    pg = PostgresHook(postgres_conn_id=CONN_ID)
    if rows:
        pg.insert_rows("raw.weather_data_hourly", [row.values() for row in rows], target_fields=list(rows[0].keys()), replace=True,
                       replace_index=["jaam_kood", "aasta", "kuu", "paev", "tund", "element_kood"])
def get(table, params, retries=3):
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


def fetch_month(stations, year, month, elements = DEFAULT_ELEMENTS):
    params = {
        "aasta": f"eq.{year}",
        "kuu": f"eq.{month}",
        "select": "jaam_kood,jaam_nimi,aasta,kuu,paev,tund,element_kood,vaartus",
        "order": "paev,tund,jaam_kood,element_kood",
    }
    if stations:
        params["jaam_kood"] = f"in.({','.join(stations)})"
    if elements:
        params["element_kood"] = f"in.({','.join(elements)})"
    rows, offset = [], 0
    while True:
        batch = get("f_kliima_tund", {**params, "limit": PAGE, "offset": offset})
        rows.extend(batch)
        if len(batch) < PAGE:
            return rows
        offset += PAGE


def list_stations():
    rows = get("f_kliima_jaam_vaatlus", {
        "select": "jaam_kood,jaam_nimi,laiuskraad,pikkuskraad,korgus_merepinnast_m",
        "order": "jaam_nimi",
    })
    seen = {}
    for r in rows:
        seen.setdefault(r["jaam_kood"], r)
    for r in seen.values():
        print(f"{r['jaam_kood']:10} {r['jaam_nimi']:16} lat={r['laiuskraad']} lon={r['pikkuskraad']} "
              f"elev={r['korgus_merepinnast_m']} m")



with DAG(
    dag_id="fetch_weather_data_pipeline",
    description="fetch weather data",
    schedule=None,                          # trigger manually
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "weather"],
) as dag:
   t_ingest = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather_data,
        # Only this task talks to the network, so it gets the extra retries.
        retries=3,
    )
   t_ingest
   

