from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
#from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
# from helpers.redshift import getconnection
# from helpers.apogee import apogee_input_count
# from helpers.sqlserver import get_build_info
from airflow.models import Variable
from airflow.utils.email import send_email_smtp
import json
import boto3
import os.path
import logging


default_args = {
    'owner': 'sf',
    'start_date': datetime(2020, 1, 7),
    'depends_on_past': False,
    'email': Variable.get('var-email-on-failure'),
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'dbid': 74,
    'retries': 0,
    'email_on_retry': False
}

def send_email(config_file, dag, **kwargs):
    s3_object = boto3.client('s3', 'us-east-1')
    bucket_name = Variable.get('var-report-bucket-name')
    with open(config_file) as f:
        config = json.load(f)
    tmp_file_list = []
    file_ext =datetime.now().strftime('%m-%d-%Y') +'.csv'
    for item in config:
        if 'reportname' in item:
            key = config[item][1:] + '000'
            file_name = os.path.basename(key)
            logging.info(f'Report is being downloaded: {file_name}')
            tmp_file_name = f'/tmp/{file_name[:-3]}{file_ext}'
            if os.path.exists(tmp_file_name):
                os.remove(tmp_file_name)
            try:
                s3_object.download_file(bucket_name, key, tmp_file_name)
            except:
                logging.info(f'Error downloading report: {tmp_file_name}')
                continue
            tmp_file_list.append(tmp_file_name)
            logging.info(tmp_file_name)
    logging.info(tmp_file_list)

    email = EmailOperator(
            mime_charset='utf-8',
            task_id='email_task',
            to =[config['email_address']],
            #cc = [Variable.get('var-email-dworders')],
            subject=config['email_subject'],
            html_content= config['email_message'],
            files=tmp_file_list,
            dag = dag )

    email.execute(context=kwargs)


dag = DAG('z-susan-testing',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once',
          max_active_runs=1
          )

#start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# # get build
# fetch_buildinfo = PythonOperator(
#     task_id="fetch_build",
#     python_callable=get_build_info,
#     op_kwargs={'databaseid': default_args['dbid'],
#                'sql_conn_id': Variable.get('var-sqlserver-conn')
#                },
#     provide_context=True,
#     dag=dag)

t1 = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)
#
# t2 = PythonOperator(
#    task_id='displaydata',
#    python_callable=displayrecords,
#    op_kwargs={'sql': 'select * from airflow_sales'},
#    dag=dag,
# )

# t3 = PythonOperator(
#     task_id='test_apogee_proc',
#     python_callable=apogee_preupdate_audit,
#     # op_kwargs={'sql': 'select * from airflow_sales'},
#     dag=dag,
# )

# t3 = PythonOperator(
#     task_id='test_apogee_proc_from_helper',
#     python_callable=apogee_input_count,
#     op_kwargs={'buildid': Variable.get('var-db-74', deserialize_json=True)['build_id']},
#     dag=dag,
# )

# test_jdbc = PostgresOperator(
#     task_id="test_jdbc_conn",
#     dag=dag,
#     postgres_conn_id="redshift_dev",
#     sql="select * from apogee_input_count"
# )

#end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

t1