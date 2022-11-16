import os
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import awswrangler as wr
import boto3
import datetime
import pandas as pd
from concurrent import futures
import billiard as multiprocessing
import time
from helpers.send_email import send_email_without_config
#from billiard import Pool, cpu_count

default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
    "email_on_retry": False,
}

dag = DAG(
    "maxactive-run-test",
    default_args=default_args,
    description="maxactive-run-test",
    schedule_interval="@once",
    max_active_runs=2,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

def get_args(**kwargs):
    if kwargs['dag_run'].conf.get('file_key') == None:
        raise ValueError('Please pass filekey')
    else:
        file_key = kwargs['dag_run'].conf.get('file_key') #mzp_signal/ready_tablename.dat

    if kwargs['dag_run'].conf.get('bucket_name') == None:
        raise ValueError('Please pass bucket_name')
    else:
        bucket_name = kwargs['dag_run'].conf.get('bucket_name')#playgroundbucket
    
    print(file_key)
    time.sleep(30)
    
data_check = PythonOperator(
    task_id="data_check",
    python_callable=get_args,
    provide_context=True,
    dag=dag)



end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> data_check  >> end_operator