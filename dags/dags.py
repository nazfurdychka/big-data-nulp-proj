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
    spark_jobs_dir_path = os.path.join(SPARK_JOBS_PARENT_DIR_PATH, 'questions_set_1')

    spark_operator_1 = SparkSubmitOperator(
        task_id='top_genres_for_multilanguage_movies_task',
        application=os.path.join(spark_jobs_dir_path, 'top_genres_for_multilanguage_movies.py'),
        conn_id=DEFAULT_CONNECTION_ID)
    spark_operator_2 = SparkSubmitOperator(
        task_id='ratings_distribution_by_title_type_task',
        application=os.path.join(spark_jobs_dir_path, 'ratings_distribution_by_title_type.py'),
        conn_id=DEFAULT_CONNECTION_ID)
    spark_operator_3 = SparkSubmitOperator(
        task_id='genre_by_year_trends_task',
        application=os.path.join(spark_jobs_dir_path, 'genre_by_year_trends.py'),
        conn_id=DEFAULT_CONNECTION_ID)
    spark_operator_4 = SparkSubmitOperator(
        task_id='most_popular_job_in_not_adult_media_task',
        application=os.path.join(spark_jobs_dir_path, 'most_popular_job_in_not_adult_media.py'),
        conn_id=DEFAULT_CONNECTION_ID)
    spark_operator_5 = SparkSubmitOperator(
        task_id='average_age_of_actors_in_top_rated_media_task',
        application=os.path.join(spark_jobs_dir_path, 'average_age_of_actors_in_top_rated_media.py'),
        conn_id=DEFAULT_CONNECTION_ID)
    spark_operator_6 = SparkSubmitOperator(
        task_id='title_type_with_most_languages_task',
        application=os.path.join(spark_jobs_dir_path, 'title_type_with_most_languages.py'),
        conn_id=DEFAULT_CONNECTION_ID)

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator_1 >> spark_operator_2 >> spark_operator_3 >> spark_operator_4 >> spark_operator_5 >> spark_operator_6 >> end_operator

with DAG(
        'business-questions-2',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        description=f'Owned by ______, questions can be accessed by : {BUSINESS_QUESTIONS_LINK}',
        schedule_interval=None,
) as dag:
    start_operator = DummyOperator(task_id='start')
    spark_operator_1 = SparkSubmitOperator(
        task_id='spark-template',
        application=os.path.join(SPARK_JOBS_PARENT_DIR_PATH, 'spark_job_template.py'),
        conn_id=DEFAULT_CONNECTION_ID,
    )

    end_operator = DummyOperator(task_id='end')

    start_operator >> spark_operator_1 >> end_operator
