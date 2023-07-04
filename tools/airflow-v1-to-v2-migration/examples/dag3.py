from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 31),
}

def my_python_function():
    # Your Python code here
    print("Hello, Airflow!")

with DAG('python_operator_dag', default_args=default_args, schedule_interval=None) as dag:

    run_python_task = PythonOperator(
        task_id='run_python_task',
        python_callable=my_python_function,
    )

    run_python_task
