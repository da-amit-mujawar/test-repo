import datetime
import json
import os
import re
import airflow
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers.send_email import send_email

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
    "maintenance-redshift-housekeeping-old-table-drop",
    default_args=default_args,
    description="wipe redshift tables older than a specific period",
    schedule_interval="0 1 * * *",
    max_active_runs=1,
)


def wipe_table(config_file, **kwargs):
    # Checking for redshift connection
    # read argument value and initialize variables

    tables_wiped_dict = {}
    rs_conn_id = "var-redshift-da-rs-01-connection"

    table_wipe_list_query = """ SELECT nspname  AS schema_name,
                                relname         AS table_name,
                                relcreationtime AS creation_time
                         FROM pg_class_info c,
                              pg_namespace n
                         WHERE c.relnamespace = n.oid
                           AND reltype != 0
                           AND nspname = 'workspace'
                           AND creation_time < current_timestamp - 90 ;"""

    table_wipe_list = get_query_result(rs_conn_id, table_wipe_list_query)
    logging.info("table wipe fetch is :: ", table_wipe_list)
    tw_list = []
    for t in table_wipe_list:
        tw_list.append(t[0:2])

    table_list = [".".join(table) for table in tw_list]

    for idx, table in enumerate(table_list):
        wipe_sql = "DROP TABLE IF EXISTS {} ;".format(table)

        logging.info("Schema wipe started ::: {}".format(wipe_sql))
        wipe_from_redshift(rs_conn_id, wipe_sql)

        # update tables_wiped_dict
        tables_wiped_dict[idx] = table
        logging.info("Table wiped stats ::: {}".format(tables_wiped_dict))

    logging.info("Table Wipe completed Successfully")


def wipe_from_redshift(rs_connection, wipe_script):
    redshift_conn_id = Variable.get(rs_connection)
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    logging.info("Dropping table from redshift with query :: ", wipe_script)
    redshift_hook.run(wipe_script, autocommit=True)


def get_query_result(rs_connection, ddl_script):
    redshift_conn_id = Variable.get(rs_connection)
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(ddl_script)
    sources = cursor.fetchall()
    return sources


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

table_wipe = PythonOperator(
    task_id="wipe_table",
    python_callable=wipe_table,
    op_kwargs={
        "config_file": default_args["current_path"] + "/config.json",
        "dag": dag,
    },
    dag=dag,
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

start_operator >> table_wipe >> send_notification >> end_operator
# start_operator >> table_wipe >> end_operator
