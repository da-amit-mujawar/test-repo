import json
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.redshift import *
from helpers.sqlserver import get_build_info


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 1438,
    "retries": 0,
    "email": Variable.get("var-email-on-failure-datalake"),
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "builds-donorbase-load-consumer",
    default_args=default_args,
    description="create table in redshift and load file from s3 using copy command",
    schedule_interval="@once",
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={"databaseid": default_args["databaseid"]},
    provide_context=True,
    dag=dag,
)

data_load = RedshiftOperator(
    task_id="data-load",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/0*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=Variable.get("var-redshift-postgres-conn"),
)

# send success email
send_notification = PythonOperator(
    task_id="email_notification",
    python_callable=send_email,
    op_kwargs={
        "config_file": default_args["current_path"] + "/config.json",
        "dag": dag,
    },
    provide_context=True,
    dag=dag,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> fetch_buildinfo >> data_load >> send_notification >> end_operator
