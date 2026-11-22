"""dbt_silver_pipeline — builds the silver layer with dbt: dbt_run_silver >> dbt_test_silver"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_FLAGS = "--no-use-colors --project-dir /dbt --profiles-dir /dbt"

with DAG(
    dag_id="dbt_silver_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 0},
    tags=["dbt", "silver"],
) as dag:
    dbt_run_silver = BashOperator(task_id="dbt_run_silver", bash_command=f"dbt run {DBT_FLAGS} --select tag:silver")
    dbt_test_silver = BashOperator(task_id="dbt_test_silver", bash_command=f"dbt test {DBT_FLAGS} --select 'tag:silver source:raw'")
    dbt_run_silver >> dbt_test_silver