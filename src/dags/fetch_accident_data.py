from __future__ import annotations

import csv
import os
import ssl
import urllib.request
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

CONN_ID = "weather_db"
LOCAL_CSV_PATH = "/tmp/accidents_export.csv"
BATCH_SIZE = 2500

DEFAULT_ACCIDENT_DATA_URL = "https://kaart.lkf.ee/accident/export/2026"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
}

def download_accident_file(**context):
    """
    Downloads the full year accident CSV from LKF.
    """
    url = context.get("dag_run") and context["dag_run"].conf.get("url") or DEFAULT_ACCIDENT_DATA_URL
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)

    print(f"Downloading accident data from: {url}")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=ctx) as response, open(LOCAL_CSV_PATH, "wb") as out_file:
        out_file.write(response.read())

    file_size = os.path.getsize(LOCAL_CSV_PATH)
    print(f"File successfully saved to {LOCAL_CSV_PATH} ({file_size} bytes)")


def ingest_accidents_to_bronze():
    """
    Reads the downloaded CSV and upserts into raw.crashes_raw.
    - New rows are inserted with loaded_at = now().
    - Existing rows are updated with any updated information, preserving original loaded_at.
    """
    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(f"File not found: {LOCAL_CSV_PATH}")

    pg = PostgresHook(postgres_conn_id=CONN_ID)
    conn = pg.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT count(*) FROM raw.crashes_raw;")
    count_before = cursor.fetchone()[0]
    print(f"Rows in raw.crashes_raw before run: {count_before}")

    insert_sql = """
        INSERT INTO raw.crashes_raw (
            juhtumi_id,
            kuupaev,
            kellaaeg,
            nadalapaev,
            situatsioon,
            kahju_suurus,
            isikukahju,
            x_koord,
            y_koord,
            maakond,
            omavalitsus,
            asula,
            valjavotte_kuupaev
        ) VALUES %s
        ON CONFLICT (juhtumi_id) DO UPDATE SET
            kuupaev = EXCLUDED.kuupaev,
            kellaaeg = EXCLUDED.kellaaeg,
            nadalapaev = EXCLUDED.nadalapaev,
            situatsioon = EXCLUDED.situatsioon,
            kahju_suurus = EXCLUDED.kahju_suurus,
            isikukahju = EXCLUDED.isikukahju,
            x_koord = EXCLUDED.x_koord,
            y_koord = EXCLUDED.y_koord,
            maakond = EXCLUDED.maakond,
            omavalitsus = EXCLUDED.omavalitsus,
            asula = EXCLUDED.asula,
            valjavotte_kuupaev = EXCLUDED.valjavotte_kuupaev;
    """

    batch = []
    total_processed = 0

    with open(LOCAL_CSV_PATH, mode="r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)  # skip header row

        for row in reader:
            if not row or len(row) < 13:
                continue

            cleaned = [col.strip() for col in row]
            record = (
                int(cleaned[0]),  # juhtumi id
                cleaned[1],       # kuupäev
                cleaned[2],       # kellaaeg
                cleaned[3],       # nädalapäev
                cleaned[4],       # situatsioon
                cleaned[5],       # kahju_suurus
                cleaned[6],       # isikukahju
                cleaned[7],       # x_koord
                cleaned[8],       # y_koord
                cleaned[9],       # maakond
                cleaned[10],      # omavalitsus
                cleaned[11],      # asula
                cleaned[12]       # väljavõtte kuupäev
            )
            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                execute_values(cursor, insert_sql, batch)
                conn.commit()
                total_processed += len(batch)
                batch = []

        if batch:
            execute_values(cursor, insert_sql, batch)
            conn.commit()
            total_processed += len(batch)

    cursor.execute("SELECT count(*) FROM raw.crashes_raw;")
    count_after = cursor.fetchone()[0]
    new_rows = count_after - count_before

    cursor.close()
    conn.close()

    print(f"Processed {total_processed} rows from CSV.")
    print(f"Rows in DB after run: {count_after}")
    print(f"New rows added this run: {new_rows}")


with DAG(
    dag_id="ingest_accident_data_pipeline",
    description="Download LKF accident CSV and ingest new/updated rows into bronze layer every hour",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "lkf", "accidents"],
) as dag:

    download_task = PythonOperator(
        task_id="download_accident_file",
        python_callable=download_accident_file,
    )

    ingest_task = PythonOperator(
        task_id="ingest_accidents_to_bronze",
        python_callable=ingest_accidents_to_bronze,
    )

    download_task >> ingest_task