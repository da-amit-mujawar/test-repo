import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import *
from helpers.redshift import *

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'database_id': 0,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}
dag = DAG('marketteam-apogee-export',
    default_args=default_args,
    description='MarketTeam Coop Exports',
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='load-consumer-strip-file',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/01*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load_export = RedshiftOperator(
    task_id='load-export-coop-files',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/02*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
# check counts
data_check = DataCheckOperator(
    task_id='validating-record-count',
    dag=dag,
    config_file_path=default_args['currentpath'] + '/config.json',
    min_expected_count=[('tablename1', 120000000)],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
   task_id='email-notification',
   python_callable=send_email,
   op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
              'dag': dag
              },
   dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_load >> data_load_export>> data_check >> send_notification >> end_operator
