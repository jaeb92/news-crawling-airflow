import os
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

today = datetime.today() - timedelta(1)
yesterday = datetime.strftime(today, '%Y%m%d')

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="crawling_news",
    start_date=datetime(2023, 9, 1, tzinfo=KST),
    catchup=True,
    schedule="10 0 * * *"
) as dag:
    
    task1 = BashOperator(
        task_id='politics',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c politics -d {yesterday}"
    )
    task2 = BashOperator(
        task_id='economy',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c economy -d {yesterday}"
    )
    task3 = BashOperator(
        task_id='international',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c international -d {yesterday}"
    )
    task4 = BashOperator(
        task_id='society',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c society -d {yesterday}"
    )
    task5 = BashOperator(
        task_id='culture',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c culture -d {yesterday}"
    )
    task6 = BashOperator(
        task_id='entertainment',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c entertainment -d {yesterday}"
    )
    task7 = BashOperator(
        task_id='sports',
        bash_command=f"cd /opt/airflow/ && python src/crawling.py -s hankook -c sports -d {yesterday}"
    )
    
    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7