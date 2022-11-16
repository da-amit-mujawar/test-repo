import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator
from helpers.send_email import send_email
from datetime import datetime

default_args = {
    'owner': 'data-axle',
    'start_date': datetime(2021, 5, 3),
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email-failure': Variable.get('var-email-on-failure-datalake'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('datalake-internal-create-spectrumlinks',
    default_args=default_args,
    description='Create RedShift Spectrum External Tables from S3',
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = RedshiftOperator(
    task_id='create_tables',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get("var-redshift-da-rs-01-connection")
)

send_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables >> send_notification >> end_operator
