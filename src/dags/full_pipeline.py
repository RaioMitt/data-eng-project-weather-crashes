"""full_pipeline — runs the whole pipeline in one DAG: bronze -> silver -> gold.

    ingest_station_data ─────────────────────────┐
    ingest_weather_data ─────────────────────────┼─> dbt_run_silver >> dbt_test_silver
    download_accident_file >> ingest_accidents ──┘       >> dbt_run_gold >> dbt_test_gold

The three bronze loads run in parallel; silver starts only when all of them succeed.
The tasks reuse the functions and dbt commands of the single-step DAGs in steps/,
which can still be triggered on their own (e.g. to rebuild only gold).

Schedule: manual trigger only. Optional run config: {"url": "..."} overrides the
accident CSV URL, the same as in ingest_accident_data_pipeline.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from steps.dbt_gold_pipeline import RUN_GOLD, TEST_GOLD
from steps.dbt_silver_pipeline import RUN_SILVER, TEST_SILVER
from steps.fetch_accident_data import download_accident_file, ingest_accidents_to_bronze
from steps.fetch_station_data import ingest_station_data
from steps.fetch_weather_data import ingest_weather_data

with DAG(
    dag_id="full_pipeline",
    description="bronze (stations, weather, accidents) -> silver -> gold",
    schedule="@daily",                          # trigger manually
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 0},
    tags=["full", "bronze", "silver", "gold"],
) as dag:
    # Bronze: network tasks get retries, the same as in their own DAGs.
    t_stations = PythonOperator(
        task_id="ingest_station_data",
        python_callable=ingest_station_data,
        retries=3,
    )
    t_weather = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather_data,
        retries=3,
    )
    t_download_accidents = PythonOperator(
        task_id="download_accident_file",
        python_callable=download_accident_file,
        retries=2,
        retry_delay=pendulum.duration(minutes=2),
    )
    t_ingest_accidents = PythonOperator(
        task_id="ingest_accidents_to_bronze",
        python_callable=ingest_accidents_to_bronze,
        retries=2,
        retry_delay=pendulum.duration(minutes=2),
    )

    # Silver and gold (dbt). A failed test stops the run before the next layer.
    dbt_run_silver = BashOperator(task_id="dbt_run_silver", bash_command=RUN_SILVER)
    dbt_test_silver = BashOperator(task_id="dbt_test_silver", bash_command=TEST_SILVER)
    dbt_run_gold = BashOperator(task_id="dbt_run_gold", bash_command=RUN_GOLD)
    dbt_test_gold = BashOperator(task_id="dbt_test_gold", bash_command=TEST_GOLD)

    t_download_accidents >> t_ingest_accidents
    [t_stations, t_weather, t_ingest_accidents] >> dbt_run_silver
    dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold
