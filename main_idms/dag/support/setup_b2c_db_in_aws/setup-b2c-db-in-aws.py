import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from support.setup_b2c_db_in_aws.tasks import *
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


dag = DAG('support-setup-b2c-db-in-aws',
          default_args=default_args,
          description='support setup b2c db in aws',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


setup_b2c_db_in_aws = PythonOperator(
    task_id='setup_b2c_db_in_aws_sql',
    python_callable= setup_b2c_db_in_aws_sql,
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

start_operator >> setup_b2c_db_in_aws  >> end_operator
