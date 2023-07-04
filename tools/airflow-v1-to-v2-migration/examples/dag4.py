from airflow import DAG
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('gcs_list_operator_dag', default_args=default_args, schedule_interval=None) as dag:

    list_files_task = GoogleCloudStorageListOperator(
        task_id='list_files_task',
        bucket='your_bucket_name',
        prefix='your_prefix',
        delimiter='/',
        google_cloud_storage_conn_id='google_cloud_default',  # Replace with your GCS connection ID
    )

    list_files_task
