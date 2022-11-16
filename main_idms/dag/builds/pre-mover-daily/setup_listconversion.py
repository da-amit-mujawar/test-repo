import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.redshift import get_redshift_hook
from helpers.sqlserver import get_sqlserver_hook
from datetime import datetime
from contextlib import closing
import json
import logging

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-pre-mover-daily-load',
          default_args=default_args,
          description='build-premover-load-daily',
          schedule_interval='@once',
          max_active_runs=1
          )


def update_listload_status_script(config_file,  **kwargs):
    # log the details
    logging.info(f"Setting up the list for aws list conversion....")

    # read the script file
    scriptfilename = default_args['current_path'] + \
        '/010-get-listconversion-script.sql'
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()
    # get current date
    current_date = datetime.now()
    strScript = strScript.replace('{dd}', current_date.strftime('%d'))
    strScript = strScript.replace('{mm}', current_date.strftime('%m'))
    strScript = strScript.replace('{yyyy}', current_date.strftime('%Y'))

    # read the variable
    config = Variable.get('var-db-1321', deserialize_json=True)
    logging.info(f"{config}")

    for item in config:
        strScript = strScript.replace('{' + item + '}', str(config[item]))

    s3_bucket_name = Variable.get('var-s3-bucket-names', deserialize_json=True)

    for bucket in s3_bucket_name:
        strScript = strScript.replace(
            '{' + bucket + '}', s3_bucket_name[bucket])

    logging.info(f"script:  {strScript}")

    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    with closing(sql_conn.cursor()) as sql_cursor:
        sql_cursor.execute(strScript)

    logging.info(f"Completed.")


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
updateliststatus = PythonOperator(
    task_id='updateliststatus_ultimatenewmover',
    python_callable=update_listload_status_script,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag,
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> updateliststatus >> send_notification >> end_operator
