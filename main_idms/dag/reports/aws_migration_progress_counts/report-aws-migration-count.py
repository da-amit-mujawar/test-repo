import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.aws_migration_progress_counts.tasks import *
import logging


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False   
}


dag = DAG('reports-aws-migration-counts',
          default_args=default_args,
          description='reports aws migration counts',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


get_aws_migration_counts_sql = PythonOperator(
    task_id='get_aws_migration_counts_sql',
    python_callable=get_aws_migration_counts_sql,
    op_kwargs={'dag': dag,
            'config_file': default_args['currentpath'] + '/config.json'},          
    dag=dag,
)


# send_notification = PythonOperator(
#     task_id='email_notification',
#     python_callable=send_email,
#     op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
#                'dag': dag
#                },
#     dag=dag)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> get_aws_migration_counts_sql  >> end_operator
