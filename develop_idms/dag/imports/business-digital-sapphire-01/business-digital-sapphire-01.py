import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from operators.redshift import *
from helpers.send_email import send_email
from helpers.sqlserver import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 71,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('imports-business-digital-sapphire-01',
          default_args=default_args,
          description='imports business-digital-sapphire-01',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='loading-import-file',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/0*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    # redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

fetch_buildinfo = PythonOperator(
  task_id="fetch_build",
  python_callable=get_build_info,
  op_kwargs={'databaseid': default_args['databaseid'],
             'active_flag': 1
             },
  provide_context=True,
  dag=dag)


# check counts
data_check = DataCheckOperator(
    task_id='validating-record-count',
    dag=dag,
    config_file_path=default_args['current_path'] + '/config.json',
    min_expected_count=[
                            ('table_sap_main', 100000),
                            ('table_sap_email', 100000),
                            ('table_sap_hardware', 100000),
                            ('table_sap_software', 100000),
                            ('table_sap_industry', 100000),
                            ('table_sap_interestarea', 100000),
                            ('table_sap_jobfunction', 100000),
                            ('table_sap_locationtype', 100000),
                            ('table_sap_productspecified', 100000),
                            ('table_sap_specialty', 100000)
                        ],
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    # redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

activate = RedshiftOperator(
    task_id='rename-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/*rename.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    # redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> data_load >> data_check >> activate >> send_notification >> end_operator

