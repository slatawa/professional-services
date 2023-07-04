from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

with DAG('trigger_dagrun_operator_dag', default_args=default_args, schedule_interval=None) as dag:

    trigger_dagrun_task = TriggerDagRunOperator(
        task_id='trigger_dagrun_task',
        trigger_dag_id='your_target_dag_id',
        execution_date='{{ execution_date }}',
    )

    trigger_dagrun_task
