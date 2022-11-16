import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from reports.amerimark_shipping_report_daily.tasks import generate_report, clean_up_sql
from helpers.send_email import send_email
from datetime import datetime, timedelta


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'param_begin_date': '',
    'param_end_date': '',
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

# Setting start and end dates.
start_date = default_args['param_begin_date']
end_date = default_args['param_end_date']
if not start_date.strip():
    start_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
if not end_date.strip():
    end_date = datetime.today().strftime('%Y%m%d')

report_path = f'/tmp/1008_ShippingReport_{start_date}_To_{end_date}.csv'

dag = DAG('reports-amerimark-shipping-report-daily',
          default_args=default_args,
          description='AMERIMARK SHIPPING REPORT - DAILY (MON-SAT)',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    op_kwargs={'start_date': start_date,
               'end_date': end_date,
               'report_path': report_path
               },
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

clean_up_sql = PythonOperator(
    task_id='clean_up_sql',
    python_callable=clean_up_sql,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> generate_report >> send_notification >> clean_up_sql >> end_operator
