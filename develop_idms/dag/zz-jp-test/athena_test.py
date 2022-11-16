import logging
import os
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from helpers.send_email import send_email_without_config
from airflow.hooks.postgres_hook import PostgresHook
import awswrangler as wr
import boto3
from builds.nonprofit_validate_export_data import tasks
from operators import RedshiftOperator
from helpers.s3 import getObjectList
import datetime, random
import pandas as pd
from concurrent import futures


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "zz-jp-athena-test",
    default_args=default_args,
    description="athen multi-threading test",
    schedule_interval="@once",
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


def runsql(sql):
    df_athena = wr.athena.read_sql_query(
        sql=sql,
        database="develop_idms",
        s3_output="s3://develop_idms-2722-athena",
        use_threads=True,
    )
    print(df_athena)


def export_data1(**kwargs):
    sql = f"SELECT accountno nCount FROM develop_idms.jp_test_1 limit {str(random.choice([1, 4, 8, 10, 3]))}"
    runsql(sql)


def export_data2(**kwargs):
    sql = f"SELECT accountno nCount FROM develop_idms.jp_test_1 limit {str(random.choice([1, 4, 8, 10, 3]))}"
    runsql(sql)


def export_data3(**kwargs):
    sql = f"SELECT accountno nCount FROM develop_idms.jp_test_1 limit {str(random.choice([1, 4, 8, 10, 3]))}"
    runsql(sql)


def export_data4(**kwargs):
    sql = f"SELECT accountno nCount FROM develop_idms.jp_test_1 limit {str(random.choice([1, 4, 8, 10, 3]))}"
    runsql(sql)


def export_data5(**kwargs):
    sql = f"SELECT accountno nCount FROM develop_idms.jp_test_1 limit {str(random.choice([1, 4, 8, 10, 3]))}"
    runsql(sql)


def export_all(**kwargs):
    test_dict = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    print("before futures")
    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        print("in futures")
        result = executor.map(export_data, test_dict)


"""
export = PythonOperator(
    task_id="export_all",
    python_callable=export_all,
    provide_context=True,
    dag=dag,
)
"""

export1 = PythonOperator(
    task_id="export_data1",
    python_callable=export_data1,
    provide_context=True,
    dag=dag,
)

export2 = PythonOperator(
    task_id="export_data2",
    python_callable=export_data2,
    provide_context=True,
    dag=dag,
)

export3 = PythonOperator(
    task_id="export_data3",
    python_callable=export_data3,
    provide_context=True,
    dag=dag,
)

export4 = PythonOperator(
    task_id="export_data4",
    python_callable=export_data4,
    provide_context=True,
    dag=dag,
)

export5 = PythonOperator(
    task_id="export_data5",
    python_callable=export_data5,
    provide_context=True,
    dag=dag,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> [export1, export2, export3, export4, export5] >> end_operator
