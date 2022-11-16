import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.alert_missing_amex_ids_file.tasks import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1150,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False   
}


dag = DAG('reports-alert-missing-amex-ids-file',
          default_args=default_args,
          description='ALERT MISSING AMEX IDS FILE',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


get_file_data_in_table_sql = PythonOperator(
    task_id='get_file_data_in_table_sql',
    python_callable=get_file_data_in_table_sql,
    op_kwargs={'dag': dag,
            'config_file': default_args['currentpath'] + '/config.json'},          
    dag=dag,
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> get_file_data_in_table_sql >> end_operator
