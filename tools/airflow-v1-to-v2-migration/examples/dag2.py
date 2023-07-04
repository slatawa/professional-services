from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('dataproc_pyspark_dag', default_args=default_args, schedule_interval=None) as dag:

    run_pyspark_job = DataProcPySparkOperator(
        task_id='run_pyspark_job',
        main='gs://your_bucket/pyspark_job.py',  # Path to your PySpark script
        cluster_name='your_cluster_name',
        region='your_region',
        dataproc_jars=['gs://your_bucket/extra_jar.jar'],  # Optional: additional JAR files
        dataproc_pyspark_properties={'spark.executor.memory': '2g'},  # Optional: Spark properties
    )

    run_pyspark_job
