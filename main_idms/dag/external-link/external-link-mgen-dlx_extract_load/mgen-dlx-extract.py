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

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid':847,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('external-link-mgen-dlx-extract-load',
    default_args=default_args,
    description='Load data',
    schedule_interval='@once',
    max_active_runs=1
    )

def fetch_buildinfo():
    fetch_buildinfo_df = get_build_info_plus(
        databaseid=default_args["databaseid"], active_flag=1,latest_maintable_dbid = 847
        
    )
    print(fetch_buildinfo_df)
    return fetch_buildinfo_df

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
fetch_buildinfo = PythonOperator(
  task_id="fetch_build",
  python_callable= fetch_buildinfo,
  provide_context=True,
  dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='processing_list_update',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/01*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


data_check = DataCheckOperator(
    task_id='check_maintable_record_count',
    dag=dag,
    config_file_path = default_args['currentpath'] + '/config.json',
    min_expected_count = [
                            ('tablename1', 2500000)
                        ],
    #redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

rename_file = RedshiftOperator(
    task_id='rename-tableS',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/020-rename.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'reportname': ["/Reports/mgen_dlx_extract_sample_output"],
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> data_load >> data_check >> rename_file >> send_notification >> end_operator

