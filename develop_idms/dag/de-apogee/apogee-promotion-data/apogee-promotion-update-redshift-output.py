import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
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

dag = DAG('de-apogee-promotion-update-redshift-output',
          default_args=default_args,
          description='Apend Household and Individual_ID and output file',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

#load aop returned file and update redshift tables
apo_update = RedshiftOperator(
    task_id='load-aop-updade-mailfiles',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/300*.sql',
    config_file_path = default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

#unload files to s3 in parquet format
output_file = RedshiftOperator(
    task_id='output-apogee-promotion-data',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/900*.sql',
    config_file_path = default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> apo_update  >> output_file >> end_operator
