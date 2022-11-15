from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os, subprocess
import json
import boto3
from datetime import datetime
import calendar
from operators import RedshiftOperator, GenericRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 9),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

dag = DAG('z-kr-python-test',
          default_args=default_args,
          description='Test DAG for bash operator',
          schedule_interval='@once',
          max_active_runs=1
          )

config_file_path=default_args['currentpath'] + '/config.json'
with open(config_file_path) as f:
    config = json.load(f)

def pyfunction():
    cmd = "aws s3 cp s3://idms-2722-internalfiles/transfomer_dev/input/changes/20210205/Business-00000_images.txt.gz -| zcat | head -n 1"
    ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = ps.communicate()[0]
    res = output.decode("utf-8")
    reslist = res.split("\n",1)
    print('output is: ',reslist[0].strip() )

python_step = PythonOperator(
    task_id='python_task',
    python_callable=pyfunction,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)


python_step