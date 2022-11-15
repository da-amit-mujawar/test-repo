import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.haystaq_royalty_reports_do_not_run_manually.tasks import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1150,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False,
    'daily_monthly': 'D'
}

daily_monthly = default_args.get('daily_monthly', 'D')

dag = DAG('reports-haystaq-royalty-reports-do-not-run-manually',
          default_args=default_args,
          description='HAYSTAQ ROYALTY REPORT DO NOT RUN MANUALLY',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


export_haystaq_royalty_from_sql = PythonOperator(
    task_id='export_haystaq_royalty_from_sql',
    python_callable=export_haystaq_royalty_from_sql,
    op_kwargs={'daily_monthly': daily_monthly},
    dag=dag,
)

cleanup_sql = PythonOperator(
    task_id='cleanup_sql',
    python_callable=cleanup_sql,
    dag=dag,
)


send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> export_haystaq_royalty_from_sql >> cleanup_sql >> send_notification >> end_operator
