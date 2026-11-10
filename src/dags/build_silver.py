import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def run_silver():
    sql = open("/opt/airflow/schema/silver.sql", encoding="utf-8").read()
    PostgresHook(postgres_conn_id="weather_db").run(sql)

with DAG("build_silver_pipeline", schedule=None,
         start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False) as dag:
    PythonOperator(task_id="build_silver", python_callable=run_silver)