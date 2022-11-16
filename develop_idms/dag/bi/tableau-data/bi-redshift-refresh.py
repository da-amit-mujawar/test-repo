import os
from datetime import datetime
from operators import RedshiftOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.send_email import send_email

default_args = {
    "owner": "sri",
    "start_date": datetime(2021, 8, 1),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email-failure": Variable.get('var-email-on-failure-datalake'),
    "email": "DL-DataEngneering@data-axle.com",
    "retries": 0,
    "email_on_retry": False,
}

prefix = "ddl_tables/"

dag = DAG(
    "de-internal-bi-redshfit-refresh",
    default_args=default_args,
    description="Refreshes report schema tables in Redshift with daily interna Datalake tables",
    schedule_interval='0 7 * * *',
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="begin_execution", dag=dag)

send_notification = PythonOperator(
        task_id='email_notification',
        python_callable=send_email,
        op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
                  'dag': dag
               },
        dag=dag)

# run post-process sql scripts for bi and other groups
refresh_process = RedshiftOperator(
    task_id='refresh_bi_objects',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

end_operator = DummyOperator(task_id="end_execution", dag=dag)

start_operator >> refresh_process >> send_notification >> end_operator
