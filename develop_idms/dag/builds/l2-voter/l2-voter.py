import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import get_build_info
from helpers.redshift import countquery
from time import *
from helpers.sqlserver import activate_build

default_args = {
    'owner': 'axle-etl-cb',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1449,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-l2-voter',
          default_args=default_args,
          description='builds l2 voter and tblext45',
          schedule_interval='@once',
          tags=['prod'],
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# get build
fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['databaseid'],'active_flag':1},
    provide_context=True,
    dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='loading-data-to-L2-Voter',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/0*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': Variable.get('var-db-1449', deserialize_json=True)['maintable_name']},
    provide_context=True,
    dag=dag)

build_activate = PythonOperator(
    task_id="activate_build",
    python_callable=activate_build,
    op_kwargs={'database_id': default_args["databaseid"],
               'build_status': 70,
               'min_expected_count': 150000000,
               'count_task_id': 'count_maintable',
               'sql_conn_id': Variable.get('var-sqlserver-conn')
               },
    provide_context=True,
    dag=dag)

build_activate.post_execute = lambda **x: sleep(600)


# send success email
send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> data_load >> get_count >> build_activate >> send_notification >> end_operator