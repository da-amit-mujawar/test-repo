from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from operators import RedshiftOperator, DataCheckOperator
from airflow.models import Variable
from helpers.prospector import *
from helpers.sqlserver import get_build_info
from helpers.send_email import send_email
import os

default_args = {
    'owner': 'Data-Axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email': Variable.get('var-email-on-failure'),
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'dbid': 1094,
    'retries': 0,
    'email_on_retry': False
}

dag = DAG('builds-prospector',
          default_args=default_args,
          description='Load records into prospector database',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['dbid'] },
    provide_context=True,
    dag=dag)

load_main_table = PythonOperator(
    task_id='creat-and-load-main-table',
    python_callable=prospector_load_table,
    op_kwargs={'buildid': Variable.get('var-db-1094', deserialize_json=True)['build_id'],
               'databaseid': default_args['dbid'],
               'maintablename': Variable.get('var-db-1094', deserialize_json=True)['maintable_name']
                },
    dag=dag,
)

transform_task = RedshiftOperator(
    task_id='perform_calculations',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    databaseid = default_args['dbid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': Variable.get('var-db-1094', deserialize_json=True)['maintable_name']
               },
    provide_context=True,
    dag=dag)

activate = PythonOperator(
    task_id="activate_build",
    python_callable=activate_build,
    op_kwargs={'database_id': default_args["dbid"],
               'build_status':70,
               'min_expected_count': 10000000,
               'count_task_id': 'count_maintable',
               'sql_conn_id': Variable.get('var-sqlserver-conn')
               },
    provide_context=True,
    dag=dag)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
              # 'report_list': [Variable.get('var-db-1094', deserialize_json=True)['load-error-report']],
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> load_main_table >> transform_task >> get_count >> activate >> send_notification >> end_operator
