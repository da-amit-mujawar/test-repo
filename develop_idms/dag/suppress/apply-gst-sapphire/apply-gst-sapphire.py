import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import get_build_info


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(8),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 71,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('suppress-apply-gst-sapphire',
          default_args=default_args,
          description='apply gst to emails on db71',
          tags=['prod'],
          schedule_interval='0 6 * * 4',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['databaseid'],'active_flag': 1},
    provide_context=True,
    dag=dag)

apply_gst = RedshiftOperator(
    task_id='apply_gst_suppressions',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> fetch_buildinfo >> apply_gst >> send_notification >> end_operator