import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

DEFAULT_CONNECTION_ID = 'spark_default'
SPARK_JOBS_PARENT_DIR_PATH = os.path.join('/opt', 'spark_apps')
BUSINESS_QUESTIONS_LINK = 'https://docs.google.com/document/d/1y8T0asC7HBBuPaWgVDPY9YFUX3ETVNiRjbvvQgXN9qM'

default_args = {
    'owner': 'airflow',
    'retries': None,
}

with DAG(
        'business-questions-1',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        description=f'Owned by Nazar Furdychka, questions can be accessed by : {BUSINESS_QUESTIONS_LINK}',
        schedule_interval=None,
) as dag:
    start_operator = DummyOperator(task_id='start')
    spark_operator = SparkSubmitOperator(
        task_id='spark-template',
        application=os.path.join(SPARK_JOBS_PARENT_DIR_PATH, 'spark_job_template.py'),
        conn_id=DEFAULT_CONNECTION_ID,
    )

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator >> end_operator

with DAG(
        'business-questions-2',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        description=f'Owned by ______, questions can be accessed by : {BUSINESS_QUESTIONS_LINK}',
        schedule_interval=None,
) as dag:
    start_operator = DummyOperator(task_id='start')
    spark_operator = SparkSubmitOperator(
        task_id='spark-template',
        application=os.path.join(SPARK_JOBS_PARENT_DIR_PATH, 'spark_job_template.py'),
        conn_id=DEFAULT_CONNECTION_ID,
    )

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator >> end_operator