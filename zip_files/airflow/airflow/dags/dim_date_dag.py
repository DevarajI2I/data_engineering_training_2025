from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_scripts.date_etl import run_date_etl
from etl_logger import log_etl_run

def on_failure_callback(context):
    log_etl_run("dim_date", status="failed")

with DAG(
    dag_id="dim_date_dag",
    start_date=datetime(2025,10,10),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "dim_date"],
) as dag:
    load_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=run_date_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("dim_date", status="success"),
    )
