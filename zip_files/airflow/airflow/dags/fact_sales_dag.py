from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_scripts.sales_etl import run_sales_etl
from etl_logger import log_etl_run

def on_failure_callback(context):
    log_etl_run("fact_sales", status="failed")

with DAG(
    dag_id="fact_sales_dag",
    start_date=datetime(2025,10,10),
    schedule_interval=None,
    #schedule_interval='*/5 * * * *',
    catchup=False,
    tags=["etl", "fact_sales"],
) as dag:
    load_sales = PythonOperator(
        task_id="load_fact_sales",
        python_callable=run_sales_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("fact_sales", status="success"),
    )
