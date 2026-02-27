"""
A simple Airflow DAG that prints Hello World.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello World from Airflow on AKS!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'hello_world_demo',
    default_args=default_args,
    description='A simple Hello World DAG for AKS demo',
    schedule_interval=None,
    catchup=False,
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=hello_world,
    dag=dag,
)
