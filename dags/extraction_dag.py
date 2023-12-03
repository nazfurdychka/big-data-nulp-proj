import gzip
import os
import shutil
import requests
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from bs4 import BeautifulSoup

default_args = {
    'owner': 'airflow',
    'retries': None,
}

DOWNLOAD_URL = 'https://datasets.imdbws.com'
DOWNLOAD_PATH = os.path.join('/opt', 'data')


def get_download_urls(**kwargs):
    response = requests.get(DOWNLOAD_URL)
    assert response.status_code == 200
    soup = BeautifulSoup(response.text, 'html.parser')

    urls = []
    for ul in soup.find_all('ul'):
        for a in ul.find_all('a'):
            urls.append(a.get('href'))

    print(f'URLs to be processed: {urls}')
    if kwargs:
        kwargs['ti'].xcom_push(key='urls', value=urls)
    else:
        return urls


def url_to_entity_name(_url: str):
    file_name = os.path.split(_url)[-1]
    file_name = file_name.rsplit('.tsv.gz', 1)[0]
    entity_name = file_name.replace('.', '-')
    return entity_name


def url_to_archive_name(_url: str):
    archive_name = os.path.split(_url)[-1]
    return archive_name


def check_data_local_presence(**kwargs):
    urls = kwargs['ti'].xcom_pull(task_ids='get-download-urls', key='urls')
    needed_file_names = [url_to_archive_name(url).rstrip('.gz') for url in urls]
    to_be_downloaded = set(needed_file_names).difference(os.listdir(DOWNLOAD_PATH))

    if to_be_downloaded:
        print(f'Files to be downloaded: {to_be_downloaded}')
        kwargs['ti'].xcom_push(key='to_be_downloaded', value=to_be_downloaded)
        return 'start-downloading'
    else:
        return 'end'


def download_file_by_url(_url: str, **kwargs):
    file_name = url_to_archive_name(_url)
    to_be_downloaded = kwargs['ti'].xcom_pull(task_ids='check-local-data-presence', key='to_be_downloaded')
    if file_name.rstrip('.gz') not in to_be_downloaded:
        raise AirflowSkipException
    destination_file_path = os.path.join(DOWNLOAD_PATH, file_name)

    print(f'Downloading file from {_url} to {destination_file_path}')
    with requests.get(_url, stream=True) as response:
        assert response.status_code == 200

        with open(destination_file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
    print('Downloaded')


def unzip_archive(archive_path: str):
    output_path = archive_path.rstrip('.gz')
    print(f'Unzipping archive {archive_path} to {output_path}')
    with gzip.open(archive_path, 'rb') as gzipped_file:
        with open(output_path, 'wb') as output:
            shutil.copyfileobj(gzipped_file, output)
    print('Unzipped')

    print('Deleting archive file')
    os.remove(archive_path)
    print('Deleted')


with DAG(
    'extract-data',
    default_args=default_args,
    start_date=datetime.now(),
    description='DAG to download data from IMDb dataset',
    schedule_interval='@daily',
) as dag:
    start_operator = EmptyOperator(task_id='start')
    get_download_urls_operator = PythonOperator(
        task_id='get-download-urls',
        python_callable=get_download_urls,
    )
    check_data_local_presence_operator = BranchPythonOperator(
        task_id='check-local-data-presence',
        python_callable=check_data_local_presence,
    )
    start_downloading_operator = EmptyOperator(task_id='start-downloading')
    end_operator = EmptyOperator(task_id='end', trigger_rule='none_failed')

    start_operator >> get_download_urls_operator >> check_data_local_presence_operator \
                   >> [start_downloading_operator, end_operator]

    finish_downloading = EmptyOperator(task_id='finish-downloading', trigger_rule='none_failed_or_skipped')
    for url in get_download_urls():
        entity_name = url_to_entity_name(url)

        download_file_operator = PythonOperator(
            task_id=f'download-{entity_name}',
            python_callable=download_file_by_url,
            op_args=[url]
        )
        downloaded_archive_path = os.path.join(DOWNLOAD_PATH, url_to_archive_name(url))
        unzip_archive_operator = PythonOperator(
            task_id=f'unzip-{entity_name}-archive',
            python_callable=unzip_archive,
            op_args=[downloaded_archive_path]
        )

        start_downloading_operator >> download_file_operator >> unzip_archive_operator >> finish_downloading
    finish_downloading >> end_operator
