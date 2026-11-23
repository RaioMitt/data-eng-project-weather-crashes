"""dbt_silver_pipeline — builds the silver layer with dbt: dbt_run_silver >> dbt_test_silver"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_FLAGS = "--no-use-colors --project-dir /dbt --profiles-dir /dbt"
RUN_SILVER = f"dbt run {DBT_FLAGS} --select tag:silver"
TEST_SILVER = f"dbt test {DBT_FLAGS} --select 'tag:silver source:raw'"

with DAG(
    dag_id="dbt_silver_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    auto_register=False,  # full_pipeline.py imports this file; don't register this DAG there
    default_args={"owner": "airflow", "retries": 0},
    tags=["dbt", "silver"],
) as dag:
    dbt_run_silver = BashOperator(task_id="dbt_run_silver", bash_command=RUN_SILVER)
    dbt_test_silver = BashOperator(task_id="dbt_test_silver", bash_command=TEST_SILVER)
    dbt_run_silver >> dbt_test_silver