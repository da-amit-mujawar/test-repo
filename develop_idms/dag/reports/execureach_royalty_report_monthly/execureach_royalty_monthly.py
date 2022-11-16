import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.execureach_royalty_report_monthly.tasks import *
# v1.2

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('reports-execureach-royalty-monthly',
          default_args=default_args,
          description='EXECUREACH ROYALTY REPORT - MONTHLY (1ST)',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

export_fields = PythonOperator(
    task_id='export_fields',
    python_callable=export_fields,
    dag=dag,
)

export_royalty_orderids_from_sql = PythonOperator(
    task_id='export_royalty_orderids_from_sql',
    python_callable=export_royalty_orderids_from_sql,
    dag=dag,
)

# run sql scripts
import_in_redshift = RedshiftOperator(
    task_id='import_in_redshift',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_royalty_records_redshift = PythonOperator(
    task_id='get_royalty_records_redshift',
    python_callable=get_royalty_records_redshift,
    dag=dag,
)

generate_report = RedshiftOperator(
    task_id='generate_report',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/1*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)

clean_up_sql = PythonOperator(
    task_id='clean_up_sql',
    python_callable=clean_up_sql,
    dag=dag,
)

clean_up_redshift = RedshiftOperator(
    task_id='clean_up_redshift',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/2*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

delete_files = PythonOperator(
    task_id='delete_files',
    python_callable=delete_files,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> export_fields >> export_royalty_orderids_from_sql >> import_in_redshift >> get_royalty_records_redshift >> generate_report >> send_notification >> clean_up_sql >> clean_up_redshift >> delete_files >> end_operator
