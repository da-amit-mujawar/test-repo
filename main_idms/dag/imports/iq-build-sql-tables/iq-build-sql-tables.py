import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email


default_args = {
    'owner': 'axle-etl-cb',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'database_id': 0,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}
# Daily @3am
dag = DAG('imports-iq-build-sql-tables',
    default_args=default_args,
    description='imports iq-build-sql-tables',
    schedule_interval='0 3 * * *',
    tags=['prod'],
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='loading-imports-iq-build-sql-tables',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/0*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# check counts
data_check = DataCheckOperator(
    task_id='validating-record-count',
    dag=dag,
    config_file_path=default_args['current_path'] + '/config.json',
    min_expected_count=[('table_division_1', 5),
                        ('table_database_1', 1300),
                        ('table_owner_1', 2000),
                        ('table_masterlol_1', 14000),
                        ('table_build_1', 19000),
                        ('table_buildtable_1', 500),
                        ('table_buildtablelayout_1', 50000),
                        ('table_buildlol_1',300000),
                        ('table_builddd_1',600000),
                        ('table_extbuildtbldb_1', 1500),
                        ('table_dddesc_1', 25000),
                        ('table_dddesc_iq_1', 25000)
                        ],
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# rename tables
activate = RedshiftOperator(
    task_id='rename-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/*rename.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# send success email
send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_load >> data_check >> activate >> send_notification >> end_operator
