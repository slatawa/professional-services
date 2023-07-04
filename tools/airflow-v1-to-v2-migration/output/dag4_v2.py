from airflow import DAG
# Migration Utility Generated Comment -- Change Type = Changes in import , Impact = Import Statement Changed
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('gcs_list_operator_dag', default_args=default_args, schedule_interval=None) as dag:

# Migration Utility Generated Comment -- Change Type = Changes in Operator , Impact = Operator Name Change
    list_files_task = GCSListObjectsOperator(
        task_id='list_files_task',
        bucket='your_bucket_name',
        prefix='your_prefix',
        delimiter='/',
        google_cloud_storage_conn_id='google_cloud_default',  # Replace with your GCS connection ID
    )

    list_files_task
