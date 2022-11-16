import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.models import Variable
from helpers.send_email import send_email,send_email_without_config
from helpers.sqlserver import get_build_df
from airflow.decorators import dag, task
from helpers.common import prepare_sql_file_using_df
from helpers.redshift import executenonquery

default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 74,
    "retries": 0,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    dag_id="suppress-ccpa-np-apogee",
    description="NonProfit CCPA Daily Suppressions - Donor Data",
    schedule_interval="15 10 * * *",
    max_active_runs=1,
)
def pipeline():
    @task
    def fetch_buildinfo():
        fetch_buildinfo_df = get_build_df(
            databaseid=default_args["databaseid"], active_flag=1
        )
        return fetch_buildinfo_df

    @task()
    def data_load(fetch_buildinfo_df):
        sql = prepare_sql_file_using_df(
                sql_file=default_args["current_path"] + "/01*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args["current_path"] + "/config.json",
                )      
        executenonquery(sql)

    @task()
    def send_notification():
        send_email_without_config(reportname="/Reports/CCPA_Count_record_apogee",
        dag=dag,
        email_business_users=["idmsadminconsumerpearl@data-axle.com"],
        email_subject="NonProfit Daily CCPA Suppressions Apogee",
        email_message="Successfully Fetch count.<br/><br/> PFA sample output")

    (data_load(fetch_buildinfo()) >> send_notification())

ccpa_dag = pipeline()
