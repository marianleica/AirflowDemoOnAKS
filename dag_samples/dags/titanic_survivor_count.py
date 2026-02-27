"""
A simple Airflow DAG that reads the Titanic CSV and counts survivors.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def count_survivors():
    df = pd.read_csv('/dag_samples/titanic.csv')
    survivors = df[df['Survived'] == 1].shape[0]
    print(f"Number of survivors: {survivors}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'titanic_survivor_count',
    default_args=default_args,
    description='Counts Titanic survivors from CSV',
    schedule_interval=None,
    catchup=False,
)

count_task = PythonOperator(
    task_id='count_survivors',
    python_callable=count_survivors,
    dag=dag,
)
