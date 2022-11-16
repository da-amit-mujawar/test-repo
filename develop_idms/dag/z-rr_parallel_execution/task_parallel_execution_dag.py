import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__))
    # 'retries': 0,
    # 'email': Variable.get('var-email-on-failure'),
    # 'email_on_failure': True,
    # 'email_on_retry': False
}

dag = DAG('z-rr-parallel-execution',
          default_args=default_args,
          description='parallel task execution',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# create a table and load the data as a task1
data_load1 = RedshiftOperator(
    task_id='table1_in_redshift_and_loading_csv',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/task1.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

# create a table and load the data as a task2
data_load2 = RedshiftOperator(
    task_id='table2_in_redshift_and_loading_csv',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/task2.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

# create a table and load the data as a task3
data_load3 = RedshiftOperator(
    task_id='table3_in_redshift_and_loading_csv',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/task3.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

# wait for the task1,task2 and task3 after completion it will union all the 3 tables and create a new table
task4 = RedshiftOperator(
    task_id='table_load_is_done',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/task4.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

parallel_task = [data_load1,data_load2,data_load3]

start_operator >> parallel_task >> task4 >> end_operator