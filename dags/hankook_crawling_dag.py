import os
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

KST = pendulum.timezone("Asia/Seoul")
DAG_ID="hankook_crawling_news"
DEFAULT_ARGS = {
    'start_date': datetime(2023, 9, 1, tzinfo=KST),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['jaebin9274@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id=DAG_ID,
    user_defined_macros={'local_dt': lambda x: x.in_timezone(KST).strftime("%Y%m%d")},
    catchup=True,
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS
) as dag:
    
    task1 = BashOperator(
        task_id='politics',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c politics -d {{local_dt(logical_date)}}"""
    )
    task2 = BashOperator(
        task_id='economy',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c economy -d {{local_dt(logical_date)}}"""
    )
    task3 = BashOperator(
        task_id='international',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c international -d {{local_dt(logical_date)}}"""
    )
    task4 = BashOperator(
        task_id='society',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c society -d {{local_dt(logical_date)}}"""
    )
    task5 = BashOperator(
        task_id='culture',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c culture -d {{local_dt(logical_date)}}"""
    )
    task6 = BashOperator(
        task_id='entertainment',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c entertainment -d {{local_dt(logical_date)}}"""
    )
    task7 = BashOperator(
        task_id='sports',
        bash_command="""cd /opt/airflow/ && python src/crawling.py -s hankook -c sports -d {{local_dt(logical_date)}}"""
    )
    
    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7