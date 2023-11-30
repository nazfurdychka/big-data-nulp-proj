import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

DEFAULT_CONNECTION_ID = 'spark_default'
SPARK_JOBS_PARENT_DIR_PATH = os.path.join('/opt', 'spark_apps')

default_args = {
    'owner': 'airflow',
    'retries': None,
}

with DAG(
        'Question_Set_1',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        description='DAG to check custom codde',
        schedule_interval=None,
) as dag:
    start_operator = DummyOperator(task_id='start')
    spark_operator = SparkSubmitOperator(
        task_id='spark_test',
        application=os.path.join(SPARK_JOBS_PARENT_DIR_PATH, 'spark_job_template.py'),
        conn_id=DEFAULT_CONNECTION_ID,
    )

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator >> end_operator

with DAG(
        'Question_Set_2',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        description='DAG to check custom codde',
        schedule_interval=None,
) as dag:
    start_operator = DummyOperator(task_id='start')
    spark_operator = SparkSubmitOperator(
        task_id='spark_test',
        application=os.path.join(SPARK_JOBS_PARENT_DIR_PATH, 'spark_job_template.py'),
        conn_id=DEFAULT_CONNECTION_ID,
    )

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator >> end_operator
