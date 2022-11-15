from datetime import datetime, timedelta
import os,json
import boto3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 15),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

config_file=default_args['currentpath'] + '/s3_to_sftp_config.json'
with open(config_file) as f:
    config = json.load(f)

dag = DAG('sftp_dag',
          default_args=default_args,
          description='Connect to FTP source and upload file from local to sftp server',
          schedule_interval='@once',
          max_active_runs=1
          )

def download_file(bucket, key, destination):
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, key, destination)

start_operator = DummyOperator(task_id='Begin', dag=dag)

download_from_s3 = PythonOperator(
    task_id='download_from_s3',
    python_callable=download_file,
    op_kwargs={'bucket': config['source_s3_bucket'], 'key': config['source_s3_key'], 'destination': config['local_path']},
    dag=dag)

SFTP_task = SFTPOperator(
    task_id='SFTP_task',
    local_filepath=config['local_path'],
    remote_filepath=config['target_path_at_sftp'],
    ssh_conn_id=config['sftp_conn_id'],
    operation="put",
    create_intermediate_dirs=True,
    dag=dag)

remove_local_file = BashOperator(
    task_id='remove_local_file',
    bash_command='rm '+config['local_path'],
    dag=dag,
)

end_operator = DummyOperator(task_id='End', dag=dag)

start_operator >>  download_from_s3 >> SFTP_task >> remove_local_file >> end_operator
