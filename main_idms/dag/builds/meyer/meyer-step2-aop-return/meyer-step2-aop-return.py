import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from operators.redshift import *
from helpers.send_email import send_email
from helpers.sqlserver import *
from helpers.s3 import move_file

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1352,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-meyer-step2-aop-return',
          default_args=default_args,
          description='builds meyer aop step2',
          tags=['prod'],
          schedule_interval='@once',
          max_active_runs=1
          )


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# get new build info
fetch_buildinfo = PythonOperator(
  task_id="fetch_build",
    python_callable=get_build_info_plus,
    op_kwargs={'databaseid': default_args['databaseid'],
               'active_flag': 0,  # 0 or 1
               'latest_maintable_dbid': 0},  # 0 if not needed
  provide_context=True,
  dag=dag)

process_aop = RedshiftOperator(
    task_id='process-aop',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/01*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

unload_mcd = RedshiftOperator(
    task_id='unload-mcd',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/02*.sql',
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

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> process_aop >> unload_mcd >> send_notification >> end_operator
