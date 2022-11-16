import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from reports.list_updates_mgen_audit_report.tasks import *
from helpers.send_email import send_email

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('reports_list_updates_mgen_audit',
          default_args=default_args,
          description='MGen Audit Report',
          schedule_interval='@once',
          max_active_runs=1
          )
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generateAuditReport,
    op_kwargs={'buildId': None, 'dag': dag},
    dag=dag,
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'report_list': ["/tmp/Mgen_AuditReport_847_{{dag_run.conf['buildId']}}.xlsx"],
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> generate_report >> send_notification >> end_operator
