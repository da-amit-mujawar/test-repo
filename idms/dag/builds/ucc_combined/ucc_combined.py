import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import get_build_info
# from builds.ucc_combined.tasks import *


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 827,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "builds-ucc-combined",
    default_args=default_args,
    description="",
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
    task_id="data_load",
    dag=dag,
    sql_file_path=default_args["currentpath"] + "/0*.sql",
    config_file_path=default_args["currentpath"] + "/config.json",
    redshift_conn_id=Variable.get("var-redshift-etl-conn"),
)


# check counts
data_check = DataCheckOperator(
    task_id="check_maintable_record_count",
    dag=dag,
    config_file_path=default_args["currentpath"] + "/config.json",
    min_expected_count=[("tablename4", 10000000)],
    redshift_conn_id=Variable.get("var-redshift-etl-conn"),
)

activate = RedshiftOperator(
    task_id="rename-table",
    dag=dag,
    sql_file_path=default_args["currentpath"] + "/1*.sql",
    config_file_path=default_args["currentpath"] + "/config.json",
    redshift_conn_id=Variable.get("var-redshift-etl-conn"),
)

send_notification = PythonOperator(
    task_id="email-notification",
    python_callable=send_email,
    op_kwargs={"config_file": default_args["currentpath"] + "/config.json", "dag": dag},
    dag=dag,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

(
    start_operator
    >> fetch_buildinfo
    >> data_load
    >> data_check
    >> activate
    >> send_notification
    >> end_operator
)


# 992_buss_var = Variable.get('var-db-992')
# 992_buss_table = json.loads(992_buss_var)['maintable_name']
# sql = sql.replace("{992_buss_table}", 992_buss_table)
