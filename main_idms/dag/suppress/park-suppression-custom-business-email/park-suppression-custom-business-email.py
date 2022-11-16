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
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid':846,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('suppress-park-custom-business-email',
    default_args=default_args,
    description='Update Park Suppression flag for the latest build for Data Axle Business Email',
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
run_get_sql = PythonOperator(
    task_id="run_get_sql",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['databaseid'],
               'active_flag': 1
               },
    provide_context=True,
    dag=dag)

data_load = RedshiftOperator(
    task_id='processing_list_update',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> run_get_sql >> data_load >> send_notification >> end_operator

