"""
A simple Airflow DAG that demonstrates branching logic.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import random

def choose_branch():
    return 'branch_a' if random.choice([True, False]) else 'branch_b'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'branching_demo',
    default_args=default_args,
    description='A simple branching DAG for AKS demo',
    schedule_interval=None,
    catchup=False,
)

branch = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_branch,
    dag=dag,
)

branch_a = DummyOperator(task_id='branch_a', dag=dag)
branch_b = DummyOperator(task_id='branch_b', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

branch >> [branch_a, branch_b] >> end
