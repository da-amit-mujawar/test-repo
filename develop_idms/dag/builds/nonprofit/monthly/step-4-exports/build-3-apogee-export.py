import os
import airflow
import boto3 as boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator
from helpers.s3 import delete_file, copy_file
from helpers.redshift import *
from helpers.sqlserver import *
from helpers.send_email import send_email

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 74,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-nonprofit-monthly-step4-exports',
          default_args=default_args,
          description='apogee-export-mmdb-mgen-feeds',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['databaseid'],
               'active_flag': 0},
    provide_context=True,
    dag=dag)


data_work_tables = RedshiftOperator(
    task_id='create-process-tables',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/300*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


data_unload1 = RedshiftOperator(
    task_id='create-export-np-political',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/31*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_unload2 = RedshiftOperator(
    task_id='create-export-np-summary',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/32*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_unload3 = RedshiftOperator(
    task_id='create-export-np-summary-no-dedupe',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/33*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# data_unload4 = RedshiftOperator(
#     task_id='create-export-fec-summary',
#     dag=dag,
#     sql_file_path=default_args['current_path'] + '/34*.sql',
#     config_file_path=default_args['current_path'] + '/config.json',
#     databaseid=default_args['databaseid'],
#     redshift_conn_id=Variable.get('var-redshift-postgres-conn')
# )
#
#
# data_unload5 = RedshiftOperator(
#     task_id='create-export-combined-summary',
#     dag=dag,
#     sql_file_path=default_args['current_path'] + '/35*.sql',
#     config_file_path=default_args['current_path'] + '/config.json',
#     databaseid=default_args['databaseid'],
#     redshift_conn_id=Variable.get('var-redshift-postgres-conn')
# )

data_unload6 = RedshiftOperator(
    task_id='create-export-np-mgen',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/36*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_unload7 = RedshiftOperator(
    task_id='create-export-l2-voter',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/37*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# parallel_task1 = [data_unload1, data_unload2, data_unload4, data_unload5, data_unload7]
parallel_task1 = [data_unload1, data_unload2, data_unload7]

data_unload8 = RedshiftOperator(
    task_id='create-export-apg-link',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/38*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

parallel_task2 = [data_unload3, data_unload6]

data_unload9 = RedshiftOperator(
    task_id='unload-file-counts',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/39*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
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

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> data_work_tables >> parallel_task1 >> data_unload8 >> parallel_task2
parallel_task2 >> data_unload9 >> send_notification >> end_operator
