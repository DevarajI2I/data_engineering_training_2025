from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_scripts.product_etl import run_product_etl
from etl_logger import log_etl_run

def on_failure_callback(context):
    log_etl_run("dim_product", status="failed")

with DAG(
    dag_id="dim_product_dag",
    start_date=datetime(2025,10,10),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "dim_product"],
) as dag:
    load_product = PythonOperator(
        task_id="load_dim_product",
        python_callable=run_product_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("dim_product", status="success"),
    )

