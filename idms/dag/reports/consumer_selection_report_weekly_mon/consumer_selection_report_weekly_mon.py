"""
DAG failing to load. Removed by Jayesh

import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.l2_royalty_reports.tasks import *
from datetime import datetime, timedelta


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False,
    'mode': 'D'
}


#report_path = f'tmp/{report_name}'


dag = DAG('consumer_selection_report_weekly_mon',
          default_args=default_args,
          description='CONSUMER SELECTION REPORT - WEEKLY (MON)',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'report_list': [report_path],
               'dag': dag
               },
    dag=dag)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> generate_report >> send_notification >> end_operator
"""