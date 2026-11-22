"""dbt_gold_pipeline — builds the gold layer with dbt: dbt_run_gold >> dbt_test_gold"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_FLAGS = "--no-use-colors --project-dir /dbt --profiles-dir /dbt"

with DAG(
    dag_id="dbt_gold_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 0},
    tags=["dbt", "gold"],
) as dag:
    dbt_run_gold = BashOperator(task_id="dbt_run_gold", bash_command=f"dbt run {DBT_FLAGS} --select tag:gold")
    dbt_test_gold = BashOperator(task_id="dbt_test_gold", bash_command=f"dbt test {DBT_FLAGS} --select tag:gold")
    dbt_run_gold >> dbt_test_gold