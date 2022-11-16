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
import logging
from helpers.redshift import *

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('imports-dw-iusa',
          default_args=default_args,
          description='Load_Wireless_data',
          schedule_interval='@once',
          max_active_runs=1
          )

def activate_build(min_expected_count, count_task_id, **kwargs):
    table_record_count =kwargs['ti'].xcom_pull(task_ids=count_task_id)
    logging.info(f'Main table record count: {str(table_record_count)}')

    if table_record_count < min_expected_count:
        raise ValueError(f'Low Count. Please load full file. Records Loaded: {str(table_record_count)}');


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


data_load = RedshiftOperator(
    task_id='loading-tables',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={"tablename":"tblinfoUSA_US_stage"},
    provide_context=True,
    dag=dag)

Prevent_sample_load = PythonOperator(
    task_id="Prevent_sample_load",
    python_callable=activate_build,
    op_kwargs={'min_expected_count': 500000,
               'count_task_id': 'count_maintable',
               },
    provide_context=True,
    dag=dag)


rename_report = RedshiftOperator(
    task_id='data_rename_report',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/1*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
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

start_operator >> data_load >> get_count >> Prevent_sample_load >> rename_report>> send_notification >> end_operator

    



