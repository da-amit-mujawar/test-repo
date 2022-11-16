import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.all_dbs_external_link_table.tasks import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 77,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('reports-all-dbs-external-link-table',
          default_args=default_args,
          description='Reports for ALL DBs EXTERNAL LINK TABLE REPORT - MONTHLY (LAST DAY)',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
altering_views_for_pub2 = PythonOperator(
    task_id='altering_views_for_pub2',
    python_callable=altering_views_for_pub2,
    dag=dag,
)

initializing_staging_tables = PythonOperator(
    task_id='initializing_staging_tables',
    python_callable=initializing_staging_tables,
    dag=dag,
)

processing_count = PythonOperator(
    task_id='processing_count',
    python_callable=processing_count,
    dag=dag,
)

report_generation = PythonOperator(
    task_id='report_generation',
    python_callable=report_generation,
    dag=dag,
)

output_report = RedshiftOperator(
    task_id='output_report',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
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

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> altering_views_for_pub2 >> initializing_staging_tables >> processing_count >> report_generation >> output_report >> send_notification >> end_operator
