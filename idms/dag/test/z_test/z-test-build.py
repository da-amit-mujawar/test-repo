import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import get_build_info
from helpers.databricks_load import (
    alter_table,
    create_table,
    drop_table,
    load_table,
    schema_dclone_dbricks_tables,
    truncate_table,
    validate_table,
)


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 0,
    'retries': 0,
    'email': ['caroline.burch@data-axle.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('z-susan-test-build',
          default_args=default_args,
          description='test replacing build info',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# data_load = RedshiftOperator(
#     task_id='loading-input-files',
#     dag=dag,
#     sql_file_path=default_args['current_path'] + '/0*.sql',
#     config_file_path=default_args['current_path'] + '/config.json',
#     databaseid=default_args['databaseid'],
#     # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
#     redshift_conn_id=Variable.get('var-redshift-postgres-conn')
# )

# dbricks_task = PythonOperator(
#     task_id="schema_dclone_dbricks_tables",
#     python_callable=schema_dclone_dbricks_tables,
#     op_kwargs={
#         "token": Variable.get("var-secret-token-ak-databricks-dev"),
#         "config": default_args["current_path"] + "/config.json",
#         "job": "jobid",
#     },
#     provide_context=True,
#     dag=dag,
# )


notebook_run = DatabricksRunNowOperator(
    task_id="run_notebook_schema_dclone_dbricks_tables",
    databricks_conn_id ='databricks_dev',
    job_id=50,
    # notebook_params = {
    #     "dry-run": "true",
    #     "oldest-time-to-consider": "1457570074236"
    #     },
    # python_params = ["Susan Fu", "50"],
    #spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"],
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# start_operator >> fetch_buildinfo >> data_load >> data_check >> activate >> send_notification >> end_operator
#start_operator >> dbricks_task  >> end_operator
start_operator >> notebook_run  >> end_operator
