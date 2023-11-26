import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': None,
}

with DAG(
    'PYSPARK_TEST2',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    description='DAG to check the connection to Spark cluster',
    schedule_interval=None,
) as dag:
    spark_apps = os.path.join('/opt', 'spark_apps')

    spark_operator = SparkSubmitOperator(
        task_id='test_task',
        application=os.path.join(spark_apps, 'spark_app_test.py'),
        conn_id='spark_default',
        proxy_user='root'
    )
