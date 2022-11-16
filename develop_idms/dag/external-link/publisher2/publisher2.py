import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid':77,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('external-link-publisher2',
    default_args=default_args,
    description='Load publisher2 data to redshift',
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load = RedshiftOperator(
    task_id='processing_list_update',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# check counts
data_check = DataCheckOperator(
    task_id='check_maintable_record_count',
    dag=dag,
    config_file_path = default_args['currentpath'] + '/config.json',
    min_expected_count = [
                            ('tablename1', 1000000)
                        ],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

activate = RedshiftOperator(
    task_id='rename_table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/1*.sql',
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

start_operator >> data_load >> data_check >> activate >> send_notification >> end_operator

