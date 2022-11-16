import os, logging
from datetime import datetime
import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from helpers.common import prepare_sql_file_using_df
from helpers.redshift import countquery, executenonquery
from helpers.send_email import send_email_without_config
from helpers.sqlserver import get_build_df
from helpers.common import prepare_sql_file_using_df, get_df_column
from helpers.redshift import executenonquery, generate_manifest_json
from helpers.nonprofit import get_config
from helpers.s3 import get_files_using_wr
from helpers.databricks import start_databricks_job


default_args = {
    "start_date": datetime(2022, 8, 11),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 74,
    "retries": 0,
    "email": [Variable.get("var-email-on-failure")],
    "email_on_failure": True,
}


@dag(
    default_args=default_args,
    dag_id="builds-nonprofit-biweekly-process",
    description="Load child tables and other tasks that performed bi-weekly for nonprofit databases",
    schedule_interval="@once",
    max_active_runs=1,
)
def pipeline():
    current_path = os.path.abspath(os.path.dirname(__file__))
    dbid = default_args["databaseid"]
    s3_temp = get_config(dbid, "s3-temp-bucket")
    fetch_buildinfo_df = get_build_df(databaseid=dbid, active_flag=0)

    @task()
    def create_child3():
        export_bucket = get_config(dbid, "s3-silver-bucket")
        export_folder = get_config(dbid, "s3-silver-folder")
        buildid = get_df_column(fetch_buildinfo_df, "build_id")
        manifest_file_trans = f"nonprofit_trans_manifest_{buildid}.json"

        files = get_files_using_wr(f"s3://{export_bucket}{export_folder}*/*.gz")
        generate_manifest_json(s3_temp, manifest_file_trans, files)

        sql = prepare_sql_file_using_df(
            f"{current_path}/050*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
        )
        sql = sql.replace(
            "{manifest_path}", f"s3://{s3_temp}/{manifest_file_trans}"
        ).replace("{dbid}", str(dbid))

        executenonquery(sql)

    @task()
    def create_child1():
        sql = prepare_sql_file_using_df(
            sql_file=default_args["current_path"] + "/212*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
            config_file=default_args["current_path"] + "/config.json",
        )
        executenonquery(sql)

    # @task()
    # def create_child6():
    #     sql = prepare_sql_file_using_df(
    #         sql_file=default_args["current_path"] + "/26*.sql",
    #         fetch_buildinfo=fetch_buildinfo_df,
    #         config_file=default_args["current_path"] + "/config.json",
    #     )
    #     executenonquery(sql)

    @task()
    def run_databricks_job():
        domain = Variable.get("var-databricks-url")
        token = Variable.get("var-databricks-secret-token")
        if dbid == 74:
            jobid = "760562244111804"
        else:
            jobid = "TBD"

        start_databricks_job(domain, token, jobid, '{"none":0}')


    # @task()
    # def update_flags():
    #     sql = prepare_sql_file_using_df(
    #         sql_file=default_args["current_path"] + "/300*.sql",
    #         fetch_buildinfo=fetch_buildinfo_df,
    #         config_file=default_args["current_path"] + "/config.json",
    #     )
    #     executenonquery(sql)

    @task()
    def send_notification():
        send_email_without_config(
            dag=dag,
            email_subject="Airflow Notice: Nonprofit Bi-Weekly Build Process Completed: {yyyy}{mm}",
            email_message="Nonprofit Bi-Weekly Build Process Completed successfully.<br/><br/>",
        )

    (
        [
            create_child3(),
            create_child1(),
        ]
        # >> create_child6()
        >> run_databricks_job()
        # >> update_flags()
        >> send_notification()
    )


create_child_tables_dag = pipeline()
