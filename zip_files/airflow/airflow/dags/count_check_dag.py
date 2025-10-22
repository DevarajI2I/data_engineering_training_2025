from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from TableMismatchChecker import TableMismatchChecker
from TableMismatchMailHelper import TableMismatchMailHelper

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),  # Set a past date for immediate execution
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'count_check_dag',
    default_args=default_args,
    description='Script to check the count between source and destination tables',
    schedule_interval='0 0 * * *',  # Runs every day at midnight (like cron)
    catchup=False,
)

task_table_count_comparison = PythonOperator(
    task_id='table_count_comparison',
    python_callable=TableMismatchChecker().check_tables_count,
    dag=dag,
)

task_mail_triggering = PythonOperator(
    task_id = "mail_triggering",
    python_callable=TableMismatchMailHelper().mail_trigger,
    dag=dag,
)

task_table_count_comparison >> task_mail_triggering
