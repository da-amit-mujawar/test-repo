import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from airflow.operators.mssql_operator import MsSqlOperator
from helpers.send_email import send_email
from helpers.sqlserver import *
from helpers.redshift import *
from time import *

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1106,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-occupation',
          default_args=default_args,
          description='',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['databaseid']},
    provide_context=True,
    dag=dag)

data_load = RedshiftOperator(
    task_id='data_load',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# check counts
data_check = DataCheckOperator(
    task_id='check_maintable_record_count',
    dag=dag,
    config_file_path=default_args['currentpath'] + '/config.json',
    min_expected_count=[
        ('tablename1', 15000000)
    ],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

activate = RedshiftOperator(
    task_id='rename-table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/2*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': Variable.get('var-db-1106', deserialize_json=True)['maintable_name']},
    provide_context=True,
    dag=dag)

build_activate = PythonOperator(
    task_id="activate_build",
    python_callable=activate_build,
    op_kwargs={'database_id': default_args["databaseid"],
               'build_status': 61,
               'min_expected_count': 25000,
               'count_task_id': 'count_maintable',
               'sql_conn_id': Variable.get('var-sqlserver-conn')
               },
    provide_context=True,
    dag=dag)

build_activate.post_execute = lambda **x: sleep(600)

sql_command = """ EXEC Common.dbo.usp_QCBuildReport @DatabaseID = 1106, @ChangePercentage  = 5 """
Fill_rate_QC_report = MsSqlOperator( task_id = 'Fill_rate_QC_report',
                    mssql_conn_id = Variable.get('var-sqlserver-conn'),
                    sql = sql_command,
                    dag = dag,
                    autocommit = True)

send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> data_load >> data_check >> activate >> get_count >> build_activate >> Fill_rate_QC_report >> send_notification >> end_operator