from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('gcs_prefix_sensor_dag', default_args=default_args, schedule_interval=None) as dag:

    gcs_prefix_sensor_task = GoogleCloudStoragePrefixSensor(
        task_id='gcs_prefix_sensor_task',
        bucket='your_bucket_name',
        prefix='your_prefix',
        google_cloud_storage_conn_id='google_cloud_default',  # Replace with your GCS connection ID
    )

    gcs_prefix_sensor_task
