import os
import sys
from os.path import dirname, abspath

import airflow
from airflow import DAG
# from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from operators import RedshiftOperator, DataCheckOperator

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': ['susan.fu@data-axle.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('de-apogee-promotion-load-to-redshift',
          default_args=default_args,
          description='load promotion files to redshift',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# create apogee promotion s3 external tables using manifest files
create_external_tables = RedshiftOperator(
    task_id='create-apogee-promotion-s3-external-tables',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/10*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# load new splitrecaps records to Redshift
load_splitrecaps = RedshiftOperator(
    task_id='load-apogee-promotion-splitrecaps',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/120*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# load new mailfiles records to Redshift
load_mailfiles = RedshiftOperator(
    task_id='load-apogee-promotion-mailfiles',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/110*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# output new records for aop process
# NOTE: make sure s3 location is changed to s3://idms-2722-aop-input/IDMSAOPI1/ for dev
output_for_aop = RedshiftOperator(
    task_id='output-files-for-aop',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/150*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_external_tables >> load_splitrecaps >> load_mailfiles >> output_for_aop >> end_operator
