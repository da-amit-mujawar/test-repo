import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from helpers.send_email import send_email
from builds.ultimate_new_mover.part2_activate.tasks import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(8),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1333,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

report_path = f'/tmp/Logs_1333.csv'

dag = DAG('builds-ultimate-new-mover-part2-activate',
          description='builds ultimate new mover part2 activate',
          default_args=default_args,
          schedule_interval='0 1 * * 5',
          max_active_runs=1)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

build_activation = PythonOperator(
    task_id='activate_build',
    python_callable=activate_build,
    op_kwargs={'report_path': report_path},
    dag=dag
)

send_notification = PythonOperator(
    task_id='email-notification-activation',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag,
               'report_list': [report_path]
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> build_activation >> send_notification >> end_operator
