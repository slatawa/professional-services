from airflow import DAG
# Migration Utility Generated Comment -- Change Type = Changes in import , Impact = Import Statement Changed
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('dataproc_pyspark_dag', default_args=default_args, schedule_interval=None) as dag:

# Migration Utility Generated Comment -- Change Type = Changes in Operator , Impact = Operator Name Change
    run_pyspark_job = DataprocSubmitPySparkJobOperator(
        task_id='run_pyspark_job',
        main='gs://your_bucket/pyspark_job.py',  # Path to your PySpark script
        cluster_name='your_cluster_name',
        region='your_region',
        dataproc_jars=['gs://your_bucket/extra_jar.jar'],  # Optional: additional JAR files
        dataproc_pyspark_properties={'spark.executor.memory': '2g'},  # Optional: Spark properties
    )

    run_pyspark_job
