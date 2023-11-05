# import pendulum
# from datetime import datetime, timedelta

# # airflow 
# import airflow
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

# TIMEZONE = pendulum.timezone('Asia/Seoul')

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 10, 15, tzinfo=TIMEZONE),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }

# with DAG(
#     dag_id='spark_operator_sample',
#     default_args=default_args,
#     schedule_interval='@once',
#     description='use case of SparkOperator in airflow',
# ) as dag:
    
#     spark_submit = SparkSubmitOperator(
#         task_id="spark_submit",
#         conn_id="spark_local",
#         dag=dag,
#         application="/opt/airflow/src/spark/spark_basic.py"
#     )
    
#     spark_submit
    
    
# if __name__ == "__main__":
#     dag.cli()