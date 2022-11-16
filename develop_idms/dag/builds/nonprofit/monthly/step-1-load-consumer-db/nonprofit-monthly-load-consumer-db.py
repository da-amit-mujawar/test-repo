from asyncio.log import logger
from datetime import datetime
from email import header
from airflow import DAG
from operators import RedshiftOperator, DataCheckOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import os, logging
from helpers.sqlserver import get_build_df
from helpers.common import prepare_sql_file_using_df
from helpers.redshift import executenonquery
from helpers.nonprofit import get_config
from helpers.send_email import send_email_without_config

default_args = {
    "owner": "data-axle",
    "start_date": datetime(2021, 8, 7),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    dag_id="builds-nonprofit-monthly-step1-load-consumer-db",
    description="Load Consumer Universer DB into RedShift",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get(
        "s3-axle-gold"
    )
    s3_path = s3_path + "/people-mmdb-universe"

    current_path = os.path.abspath(os.path.dirname(__file__))

    # Get Inactive Build
    dbid = 74
    bdf = get_build_df(databaseid=dbid, active_flag=0)

    @task()
    def create_load_table(**kwargs):
        consumer_listid = get_config(dbid, "consumer-listid")
        sql = prepare_sql_file_using_df(f"{current_path}/01*.sql", fetch_buildinfo=bdf)
        sql = sql.replace("{consumer_listid}", str(consumer_listid)).replace(
            "{bucket_name}", s3_path
        )
        executenonquery(sql)

    @task()
    def create_matchcodes(**kwargs):
        sql = prepare_sql_file_using_df(f"{current_path}/02*.sql", fetch_buildinfo=bdf)
        executenonquery(sql)

    @task()
    def send_email(**kwargs):
        send_email_without_config(
            dag=dag,
            email_subject="Airflow Notice: Nonprofit Monthly Build Step 1 Load Consumer DB Completed - {yyyy}{mm}",
            email_message="Nonprofit Monthly Build Step 1 Load Consumer DB completed successfully.<br/><br/>",
        )

    create_load_table() >> create_matchcodes() >> send_email()


nonprofit_dag = pipeline()
