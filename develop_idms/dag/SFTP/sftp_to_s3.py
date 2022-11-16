from datetime import datetime, timedelta
import os,json
import sys
import paramiko
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.sftp_to_s3_operator import SFTPToS3Operator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

config_file=default_args['currentpath'] + '/sftp_to_s3_config.json'
with open(config_file) as f:
    config = json.load(f)

dag = DAG('sftp_to_s3',
          default_args=default_args,
          description='Connect to FTP source and download file from SFTP server to S3',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin', dag=dag)


SFTP_to_S3 = SFTPToS3Operator(
    task_id='SFTP_to_S3_task',
    s3_bucket=config['target_s3_bucket'],
    s3_key=config['target_s3_key'],
    sftp_path=config['source_path_at_sftp'],
    sftp_conn_id=config['sftp_conn_id'],
    s3_conn_id=config['s3_conn_id'],
    dag=dag)

end_operator = DummyOperator(task_id='End', dag=dag)

start_operator >>  SFTP_to_S3 >> end_operator
