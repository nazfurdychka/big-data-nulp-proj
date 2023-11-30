import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': None,
}

with DAG(
    'PySparkTest',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    description='DAG to check the connection to Spark cluster',
    schedule_interval=None,
) as dag:
    start_operator = DummyOperator(task_id='start')

    spark_apps = os.path.join('/opt', 'spark_apps')
    spark_operator = SparkSubmitOperator(
        task_id='spark_test',
        application=os.path.join(spark_apps, 'spark_app_test.py'),
        conn_id='spark_default',
    )

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator >> end_operator
