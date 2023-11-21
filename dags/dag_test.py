from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'PYSPARK_TEST',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    description='DAG to print PySpark version',
    schedule_interval=None,
) as dag:
    def get_spark_version():
        import pyspark
        print(f'Spark version - {pyspark.__version__}')

    spark_version_task = PythonOperator(
        task_id='get_spark_version',
        python_callable=get_spark_version
    )

    spark_version_task
