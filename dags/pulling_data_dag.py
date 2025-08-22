"""
    dag for pulling data from the weather api
    pulls data and puts it into a dataFrame for now
"""
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(dag_folder, '..'))
sys.path.insert(0, project_root)


import plugins.main as main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,8,22),
    'email': ['tilbo2@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG (
    'pulling_data',
    default_args=default_args,
    description='Pull data from weather api',
    schedule=timedelta(seconds=1),




) as dag: 
    test_task = PythonOperator (
        task_id = 'test',
        python_callable=main.test
    )


    test_task
#dag = DAG(
#    'pulling_data',
#    default_args=default_args,
#    description='Pull data from weather api',
#    schedule=timedelta(minutes=1),
#)

#with dag:
#    get_data_task = PythonOperator (
#        task_id = 'get_data',
#        python_callable=main.main
#    )
