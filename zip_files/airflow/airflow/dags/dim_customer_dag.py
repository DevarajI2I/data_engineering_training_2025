from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_scripts.customer_etl import run_customer_etl
from etl_scripts.product_etl import run_product_etl
from etl_scripts.date_etl import run_date_etl
from etl_scripts.sales_etl import run_sales_etl
from etl_logger import log_etl_run

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    log_etl_run("dim_customer", status="failed")
    print(f"Task {task_id} in DAG {dag_id} failed!")
    
with DAG(
    dag_id="sales_pipeline_master_dag",
    start_date=datetime(2025,10,10),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "dim_customer"],
) as dag:
    load_customer = PythonOperator(
        task_id="load_dim_customer",
        python_callable=run_customer_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("dim_customer", status="success"),
    )
    load_product = PythonOperator(
        task_id="load_dim_product",
        python_callable=run_product_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("dim_product", status="success"),
    )
    load_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=run_date_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("dim_date", status="success"),
    )
    load_sales = PythonOperator(
        task_id="load_fact_sales",
        python_callable=run_sales_etl,
        on_failure_callback=on_failure_callback,
        on_success_callback=lambda context: log_etl_run("fact_sales", status="success"),
    )    
    [load_customer, load_product, load_date] >> load_sales