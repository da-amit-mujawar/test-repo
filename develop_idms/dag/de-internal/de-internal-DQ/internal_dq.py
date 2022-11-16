from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from operators import RedshiftOperator, GenericRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import os, datetime
from datetime import datetime, timedelta
import json
from datetime import timedelta
import boto3
from botocore.client import Config
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021,2,25),
    'depends_on_past': False,
    'retries': 0,
    'email_on_retry': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
}

config_file_path=default_args['currentpath'] + '/config.json'
print(config_file_path)
with open(config_file_path) as f:
    config = json.load(f)



def create_table(**kwargs):
    print(kwargs['redshift_conn_id'])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/'

    for file_names in os.listdir(sql_file):
        fd = open(sql_file + file_names, 'r')
        sql = fd.read()
        fd.close()
        redshift_hook.run(sql)

''' Dag Creation'''
dag = DAG('de-internal-dq',
          default_args=default_args,
          description='create tables in redshift and load the data into it from s3',
          schedule_interval='@once',
          max_active_runs=1
          )

start = DummyOperator(task_id='Start', dag=dag)


create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)


end = DummyOperator(task_id='End', dag=dag)
start >>  create_table_task >> end
# start >>  load_data >> end