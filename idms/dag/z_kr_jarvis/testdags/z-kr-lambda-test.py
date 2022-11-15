"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import boto3
from datetime import datetime
from helpers.fetch_header import fetch_header
from helpers.s3 import *


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 25),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

dag = DAG('z-kr-lambda-test',
          default_args=default_args,
          description='Test DAG for lambda function',
          schedule_interval='@once',
          max_active_runs=1
          )

def driver(**kwargs):
    #Get arguments
    input = kwargs['dag_run'].conf.get('input')
    bucket_name = kwargs['dag_run'].conf.get('bucket_name')
    file_key = kwargs['dag_run'].conf.get('file_key')

    if None in (input, bucket_name, file_key):
        raise ValueError('Please pass input, bucket_name, file_key argument')

    print("Input arguments read successfully...",input," ",bucket_name," ",file_key)
    res_str = fetch_header(input, file_key,bucket_name )
    return res_str

def test_move_file():
    move_file("idms-2722-playground", "rohit_rajput/input_files/a1", "idms-2722-playground", "rohit_rajput/output_files/")


test_task = PythonOperator(
    task_id='test_task',
    python_callable=driver,
    provide_context=True,
    #dag=dag
)

test_move_file = PythonOperator(
    task_id='test_move_file',
    python_callable=test_move_file,
    provide_context=True,
    dag=dag
)
"""
