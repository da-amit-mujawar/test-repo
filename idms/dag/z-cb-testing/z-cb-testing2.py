import os
import boto3
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
# from operators import RedshiftOperator, DataCheckOperator
from operators import DataCheckOperator
from operators.redshift import *
from helpers.send_email import send_email
from helpers.sqlserver import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1150,
    'latest_maintable_dbid': 1267,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}


dag = DAG('z-cb-testing',
    default_args=default_args,
    description='z-cb-testing-dag',
    tags=['test'],
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# create the s3 resource
s3 = boto3.resource('s3')
# get the file object
#  obj = s3.Object('arn:aws:s3:::idms-2722-playground', 'cceded53-6f24-41bf-9ae2-d44a3d435302')
# read the file contents in memory
# file_contents = obj.get()["Body"].read()
# print the occurrences of the new line character to get the number of lines
# print(file_contents.count('\n'))


#fetch_buildinfo = PythonOperator(
#    task_id="fetch_build",
#    python_callable=get_build_info_plus,
#    op_kwargs={'databaseid': default_args['databaseid'],
#               'active_flag': 1,
#               'latest_maintable_dbid': default_args['latest_maintable_dbid']},
#    provide_context=True,
#    dag=dag)

'''
data_load = RedshiftOperator(
    task_id='data_load',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)
'''

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

#start_operator >> fetch_buildinfo >> data_load >> send_notification >> end_operator
start_operator >> end_operator


