import os
from datetime import datetime
from operators import RedshiftOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.databricks_load import (
    alter_table,
    create_table,
    drop_table,
    load_table,
    schema_dclone_dbricks_tables,
    truncate_table,
    validate_table,
)
from helpers.send_email import send_email

# from helpers.databricks_load import test_redshift
from sensors import DatabricksJobSensor

default_args = {
    "owner": "ak",
    "start_date": datetime(2020, 1, 7),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": "DL-DataEngneering@data-axle.com",
    "retries": 0,
    "email_on_retry": False,
}

prefix = "ddl_tables/"

dag = DAG(
    "de-internal-silvertables-load-redshift",
    default_args=default_args,
    description="Loads databricks silver tables in Redshift with Airflow",
    schedule_interval='0 7 * * *',
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="begin_execution", dag=dag)

build_schema_dclone_task = PythonOperator(
    task_id="schema_dclone_dbricks_tables",
    python_callable=schema_dclone_dbricks_tables,
    op_kwargs={
        "token": Variable.get("var-databricks-secret-token"),
        "config": default_args["currentpath"] + "/config.json",
        "job": "jobid",
    },
    provide_context=True,
    dag=dag,
)

schema_dclone_sensor_task = DatabricksJobSensor(
    task_id="schema_dclone_sensor_task",
    domain=Variable.get("var-databricks-url"),
    token=Variable.get("var-databricks-secret-token"),
    prevtaskid="schema_dclone_dbricks_tables",
    key="run_id",
    poke_interval=30,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "config": default_args["currentpath"] + "/config.json",
    },
    provide_context=True,
    dag=dag,
)

load_table_task = PythonOperator(
    task_id="load_table",
    python_callable=load_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "config": default_args["currentpath"] + "/config.json",
    },
    provide_context=True,
    dag=dag,
)

validate_table_task = PythonOperator(
    task_id="validate_table",
    python_callable=validate_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "config": default_args["currentpath"] + "/config.json",
    },
    provide_context=True,
    dag=dag,
)

send_notification = PythonOperator(
        task_id='email_notification',
        python_callable=send_email,
        op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
                  'report_list': ["s3://" + Variable.get('var-s3-bucket-names', deserialize_json=True)['s3-databricks'] + "/redshift-silver-tables-load/databricks_count.csv"],
                  'dag': dag
               },
        dag=dag)

# run post-process sql scripts for bi and other groups
post_process = RedshiftOperator(
    task_id='post_process',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

end_operator = DummyOperator(task_id="end_execution", dag=dag)

start_operator >> build_schema_dclone_task >> schema_dclone_sensor_task >> create_table_task >> load_table_task >> post_process >> validate_table_task >> send_notification >> end_operator
#start_operator >> create_table_task >> load_table_task >> post_process >> validate_table_task >> send_notification >> end_operator

#start_operator >> validate_table_task >> send_notification >> end_operator
#start_operator >> validate_table_task >> send_notification >> end_operator
#start_operator >> post_process >> send_notification >> end_operator

"""
Unused. Table being dropped in Create Table Task.

drop_table_task = PythonOperator(
    task_id="drop_table",
    python_callable=drop_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "config": default_args["currentpath"] + "/config.json",
    },
    provide_context=True,
    dag=dag,
)

alter_table_task = PythonOperator(
    task_id="alter_tables",
    python_callable=alter_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "config": default_args["currentpath"] + "/config.json",
    },
    provide_context=True,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_table,
    op_kwargs={
        "redshift_conn_id": Variable.get("var-redshift-da-rs-01-connection"),
        "prefix": prefix,
    },
    provide_context=True,
    #dag = dag
)

build_schema_dclone_struct_task = PythonOperator(
    task_id="schema_dclone_dbricks_struct_tables",
    python_callable=schema_dclone_dbricks_tables,
    op_kwargs={
        "token": Variable.get("var-secret-token-admin-databricks-prod"),
        "config": default_args["currentpath"] + "/config.json",
        "job": "struct_jobid",
    },
    provide_context=True,
    dag=dag,
)

schema_dclone_sensor_struct_task = DatabricksJobSensor(
    task_id="schema_dclone_struct_sensor_task",
    domain=Variable.get("var-databricks-url"),
    token=Variable.get("var-secret-token-admin-databricks-prod"),
    prevtaskid="schema_dclone_dbricks_struct_tables",
    key="run_id",
    poke_interval=30,
    dag=dag,
)


"""