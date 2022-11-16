import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.prospector_db_usage.tasks import *
from datetime import datetime, timedelta


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(30),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False,
}

today = datetime.today()
first = today.replace(day=1)
lastMonth = first - timedelta(days=1)
time_stamp = lastMonth.strftime("%Y%m")

report_path = f'/tmp/1094_Usage_Report_{time_stamp}.csv'

dag = DAG('reports-prospector-db-usage',
          default_args=default_args,
          description='PROSPECTOR DB USAGE REPORT - MONTHLY (1ST)',
          schedule_interval='00 10 1 * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    op_kwargs={'report_path': report_path},
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
