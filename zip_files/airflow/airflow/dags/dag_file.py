from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sampleDataScript import run_script

default_args = {
    'owner':'airflow',
    'start_date':datetime(2025,10,8),
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id='run_sample_data_every_15min',
    default_args=default_args,
    description='Runs the sample_data_script every 5 minutes',
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False,
    tags=['sample', 'testing'],
) as dag:

    run_task = PythonOperator(
        task_id='sampleDataScript',
        python_callable=run_script,
    )
