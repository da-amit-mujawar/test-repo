"""
Removed by Jayesh on 4/8/2021. DAG is failing load in airflow

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.databricks_load import (
    create_table,
)  # alter_table,; drop_table,; load_table,; truncate_table,; validate_table,
from helpers.send_email import send_email

# from helpers.databricks_load import test_redshift
from sensors import DatabricksJobSensor

default_args = {
    "owner": "ak",
    "start_date": datetime(2020, 1, 7),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": "anjani.kumar@data-axle.com",
    "retries": 0,
    "email_on_retry": False,
}

bucket = "idms-2722-databricks"
prefix = "ddl_tables/"
key = "ddl_tables/alter_table_statements.sql"

dag = DAG(
    "z-ak-location-linkage",
    default_args=default_args,
    description="Loads location linkage related tables in Redshift with Airflow",
    schedule_interval="@once",
    max_active_runs=1,
)


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

# test_redshift_task = PythonOperator(
#                         task_id = 'test_redshift',
#                         python_callable = test_redshift,
#                         op_kwargs = {'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
#                         provide_context = True,
#                         dag = dag
#  )

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "config": default_args["currentpath"] + "/config.json",
        # "bucket": bucket,
        # "prefix": prefix,
    },
    provide_context=True,
    dag=dag,
)

# validate_table_task = DummyOperator(task_id='Validate_Tables', dag=dag)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> create_table_task >> end_operator




# alter_table_task = PythonOperator(
#    task_id="alter_tables",
#    python_callable=alter_table,
#    op_kwargs={
#        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
#        "bucket": bucket,
#        "key": key,
#    },
#    provide_context=True,
#    dag=dag,
# )
#
# truncate_table_task = PythonOperator(
#    task_id="truncate_table",
#    python_callable=truncate_table,
#    op_kwargs={
#        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
#        "bucket": bucket,
#        "prefix": prefix,
#    },
#    provide_context=True,
#    #                         dag = dag
# )
#
# drop_table_task = PythonOperator(
#    task_id="drop_table",
#    python_callable=drop_table,
#    op_kwargs={
#        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
#        "config": default_args["currentpath"] + "/config.json",
#    },
#    provide_context=True,
#    # dag=dag,
# )
#
# load_table_task = PythonOperator(
#    task_id="load_table",
#    python_callable=load_table,
#    op_kwargs={
#        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
#        "config": default_args["currentpath"] + "/config.json",
#        # "bucket": bucket,
#        #        "prefix": prefix,
#    },
#    provide_context=True,
#    dag=dag,
# )
#
# validate_table_task = PythonOperator(
#    task_id="validate_tables",
#    python_callable=validate_table,
#    op_kwargs={
#        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
#        "config": default_args["currentpath"] + "/config.json",
#    },
#    provide_context=True,
#    dag=dag,
# )
#
# send_notification = PythonOperator(
#    task_id="email_notification",
#    python_callable=send_email,
#    op_kwargs={"config_file": default_args["currentpath"] + "/config.json", "dag": dag},
#    dag=dag,
# )
#
"""