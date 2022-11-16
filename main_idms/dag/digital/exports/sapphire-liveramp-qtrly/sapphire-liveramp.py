from datetime import datetime, timedelta
from multiprocessing import context
from operators import RedshiftOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers.send_email import send_email
from helpers.s3 import read_file
from helpers.databricks import start_databricks_job
from helpers.common import get_runtime_arg, prepare_sql_file
from helpers.sqlserver import get_build_info
from helpers.send_email import send_email
from airflow.decorators import dag, task
import logging
import glob
import sys
import json
import os
import airflow
from airflow.operators.email import EmailOperator

default_args = {
    "owner": "data-axle",
    "start_date": datetime(2021, 8, 7),
    "depends_on_past": False,
    "databaseid": 71,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}

"""
test change
"""

@dag(
    default_args=default_args,
    dag_id="digital-exports-sapphire-liveramp-qtrly",
    description="Digital Sapphire Quarterly Build",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    redshift_hook = PostgresHook(
        postgres_conn_id=Variable.get("var-redshift-da-rs-01-connection")
    )
    config_file = default_args["currentpath"] + "/config.json"

    @task()
    def process(**kwargs):
        """
        Fetch latest build info and loop through each SQL to execute
        """

        # Fetch Build Info
        buildinfo = get_build_info(default_args["databaseid"], 1)

        sql_files = default_args["currentpath"] + "/*.sql"

        for sql_file in sorted(glob.glob(sql_files)):
            sql = prepare_sql_file(sql_file, config_file)

            # get order id from runtime args
            order_id = get_runtime_arg("orderid", **kwargs)

            # custom variable replacements
            sql = sql.replace("{order_id}", order_id)
            sql = sql.replace("{maintable_name}", buildinfo[0])
            logging.info(f"Executing SQL {sql}")

            # uncomment the line below once testing complete
            redshift_hook.run(sql)

    send_notification = EmailOperator(
        task_id="send_notification",
        to="michael.scott@data-axle.com",
        subject="Sapphire Liveramp qtrly dag completed",
        html_content="The dag has finished",
    )

    process() >> send_notification


pipeline_dag = pipeline()
