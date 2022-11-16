import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from builds.consumer_activate.tasks import *

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'database_id': 1267,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-consumer-activate',
          default_args=default_args,
          description='builds consumer activate consumer DB (1267)',
          schedule_interval='@once',
          tags=['prod'],
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


activate_build = PythonOperator(
    task_id='activate_build',
    python_callable=activate_build,
    dag=dag,
)


# send success email
send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> activate_build >> send_notification >> end_operator