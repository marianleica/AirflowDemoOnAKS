"""
A simple Airflow DAG that demonstrates task dependencies and retries.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def always_fails():
    print("This task will fail to demonstrate retries.")
    raise Exception("Intentional failure")

def downstream_task():
    print("This runs after the retry task succeeds.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': 5,  # seconds
}

dag = DAG(
    'retry_demo',
    default_args=default_args,
    description='A DAG to show retries and dependencies',
    schedule_interval=None,
    catchup=False,
)

fail_task = PythonOperator(
    task_id='fail_and_retry',
    python_callable=always_fails,
    retries=2,
    retry_delay=5,
    dag=dag,
)

downstream = PythonOperator(
    task_id='downstream_task',
    python_callable=downstream_task,
    dag=dag,
)

fail_task >> downstream
