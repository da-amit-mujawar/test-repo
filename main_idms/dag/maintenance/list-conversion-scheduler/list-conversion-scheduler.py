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
from contextlib import closing
import json
import logging
from datetime import datetime

default_args = {
    'owner': 'data-axle',
    'start_date': datetime(2021, 8, 1),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

# 
dag = DAG('maintenance-list-conversion-scheduler',
    default_args=default_args,
    description='List Automation',
    schedule_interval='6 0 * * *',
    max_active_runs=1
    )

def schedule():
    # log the details
    logging.info(f"Setting up the list for aws list conversion....")

    # read the script file
    scriptfilename = default_args['currentpath'] + \
        '/010-execute-listConversionSchedule.sql'
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    
    logging.info(f"script:  {strScript}")

    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    with closing(sql_conn.cursor()) as sql_cursor:
        sql_cursor.execute(strScript)

    logging.info(f"Completed.")


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# 
schedule_listConversion = PythonOperator(
    task_id='schedule_listConversion',
    python_callable=schedule,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> schedule_listConversion >> end_operator

