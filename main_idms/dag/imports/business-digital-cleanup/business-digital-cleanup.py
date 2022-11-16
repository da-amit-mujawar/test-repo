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


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'database_id': 0,
    'retries': 0,
    'email': ['michael.scott@data-axle.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('imports-business-digital-cleanup',
          default_args=default_args,
          description='imports business-digital-cleanup',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='drop-import-file',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/0*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)


activate = RedshiftOperator(
    task_id='rename-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/*rename.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_load >> activate >> send_notification >> end_operator
