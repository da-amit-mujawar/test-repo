import airflow
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.s3 import move_file
from helpers.send_email import send_email
from helpers.sqlserver import *
from operators.redshift import *
from operators import RedshiftOperator, DataCheckOperator

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

dag = DAG('builds-meyer-step3-mcd-return',
          default_args=default_args,
          description='builds meyer aop step3',
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
               'latest_maintable_dbid': 0 }, #0 if not needed    provide_context=True,
    dag=dag)

data_load = RedshiftOperator(
    task_id='data_load',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/01*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

apply_mcd = RedshiftOperator(
    task_id='apply_mcd',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/02*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

apply_mcd_xref = RedshiftOperator(
    task_id='apply_mcd_xref',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/03*.sql',
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

start_operator >> fetch_buildinfo >> data_load >> apply_mcd >> apply_mcd_xref >> send_notification >> end_operator
