import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_dag",
    start_date=datetime.datetime(2023,7,25),
    schedule="@once"
):
    task1 = EmptyOperator(task_id="test_task")
    
    task1
    