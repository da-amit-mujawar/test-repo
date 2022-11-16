import os
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.send_email import send_email, send_email_without_config
from helpers.sqlserver import activate_build_without_xcom, get_build_df
from helpers.redshift import countquery, executenonquery
from airflow.decorators import dag, task
from helpers.common import prepare_sql_file_using_df

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 74,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}


@dag(dag_id="builds-apogee-step4-activate-build",
     default_args=default_args,
     description='Apogee-activate-build',
     schedule_interval='@once',
     tags=['dev'],
     max_active_runs=1
     )
def pipeline():
    @task
    def fetch_buildinfo():
        fetch_buildinfo_df = get_build_df(databaseid=default_args["databaseid"], active_flag=0)
        return fetch_buildinfo_df

    @task()
    def report_unload(fetch_buildinfo_df):
        sql = prepare_sql_file_using_df(sql_file=default_args['current_path'] + '/4*.sql',
                                        fetch_buildinfo=fetch_buildinfo_df,
                                        config_file=default_args["current_path"] + "/config.json"
                                        )
        executenonquery(sql)

    @task()
    def get_count():
        count_maintable = countquery(tablename=fetch_buildinfo(["maintable_name"][0]))
        return count_maintable

    @task()
    def build_activate(count_maintable):
        activate_build_without_xcom(database_id=default_args["databaseid"],
                                    build_status=61,
                                    min_expected_count=250000000,
                                    maintable_count=count_maintable,
                                    sql_conn_id=Variable.get('var-sqlserver-conn')
                                    )

    @task()
    def send_notification():
        send_email_without_config(reportname1="/Reports/Apogee_FinalTransactionCounts",
                                  reportname2="/Reports/Apogee_FinalTableCounts",
                                  dag=dag,
                                  email_subject="Airflow Notification : TEST-Apogee Final Build Summary Reports",
                                  email_message="Build is complete, attached are final summary reports.<br/><br/>")

    (report_unload(fetch_buildinfo)
     >> build_activate(get_count())
     >> send_notification()
     )


activate_build_dag = pipeline()
