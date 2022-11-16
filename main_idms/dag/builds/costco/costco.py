import os
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
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1078,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-costco',
          default_args=default_args,
          description='builds costco',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

fetch_buildinfo = PythonOperator(
  task_id="fetch_build",
  python_callable=get_build_info,
  op_kwargs={'databaseid': default_args['databaseid']
               },
  provide_context=True,
  dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='processing_list_update',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/0*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# check counts
data_check = DataCheckOperator(
    task_id='validating-record-count',
    dag=dag,
    config_file_path=default_args['current_path'] + '/config.json',
    min_expected_count=[('costco_tblname1', 110000000)],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

rename_report = RedshiftOperator(
    task_id='data_rename_report',
    dag=dag,
    databaseid=default_args['databaseid'],
    sql_file_path=default_args['current_path'] + '/1*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': Variable.get('var-db-1078', deserialize_json=True)['maintable_name']},
    provide_context=True,
    dag=dag)

build_activate = PythonOperator(
    task_id="activate_build",
    python_callable=activate_build,
    op_kwargs={'database_id': default_args["databaseid"],
               'build_status': 70,
               'min_expected_count': 100000000,
               'count_task_id': 'count_maintable',
               'sql_conn_id': Variable.get('var-sqlserver-conn')
               },
    provide_context=True,
    dag=dag)


# send success email
send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> data_load >> data_check >> rename_report >> get_count >> build_activate >> send_notification >> end_operator
