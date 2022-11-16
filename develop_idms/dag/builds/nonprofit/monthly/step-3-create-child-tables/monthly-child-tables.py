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
    dag_id="builds-nonprofit-monthly-step3-child-tables",
    description="Load child tables and other tasks that performed monthly for nonprofit databases",
    schedule_interval="@once",
    max_active_runs=1,
)
def pipeline():
    current_path = os.path.abspath(os.path.dirname(__file__))
    dbid = default_args["databaseid"]
    s3_temp = get_config(dbid, "s3-temp-bucket")
    fetch_buildinfo_df = get_build_df(databaseid=dbid, active_flag=0)

    # tblExternal link table export
    @task()
    def create_external39():
        sql = prepare_sql_file_using_df(
            sql_file=default_args["current_path"] + "/210*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
            config_file=current_path + "/config.json",
        )
        executenonquery(sql)

    # mgen link table
    @task()
    def create_child2():
        sql = prepare_sql_file_using_df(
            sql_file=default_args["current_path"] + "/220*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
            config_file=current_path + "/config.json",
        )
        executenonquery(sql)

    # moved to step2
    # # NP Contributor Tracking Summary
    # @task()
    # def np_contributor_summary():
    #     sql = prepare_sql_file_using_df(
    #         sql_file=default_args["current_path"] + "/230*.sql",
    #         fetch_buildinfo=fetch_buildinfo_df,
    #         config_file=current_path + "/config.json",
    #     )
    #     executenonquery(sql)

    # FEC raw transactions
    # @task()
    # def create_child4():
    #     sql = prepare_sql_file_using_df(
    #         sql_file=default_args["current_path"] + "/240*.sql",
    #         fetch_buildinfo=fetch_buildinfo_df,
    #         config_file=current_path + "/config.json",
    #     )
    #     executenonquery(sql)
    #
    # # FEC Summary
    # @task()
    # def create_child5():
    #     sql = prepare_sql_file_using_df(
    #         sql_file=default_args["current_path"] + "/250*.sql",
    #         fetch_buildinfo=fetch_buildinfo_df,
    #         config_file=current_path + "/config.json",
    #     )
    #     executenonquery(sql)

    @task()
    def create_child9():
        sql = prepare_sql_file_using_df(
            sql_file=default_args["current_path"] + "/290*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
            config_file=current_path + "/config.json",
        )
        executenonquery(sql)

    @task()
    def create_views():
        sql = prepare_sql_file_using_df(
            sql_file=default_args["current_path"] + "/300*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
            config_file=current_path + "/config.json",
        )
        executenonquery(sql)

    @task()
    def send_notification():
        send_email_without_config(
            dag=dag,
            email_subject="Airflow Notice: Nonprofit Monthly Build Step 3 Child Tables Completed - {yyyy}{mm}",
            email_message="Nonprofit Monthly Build Step 3 Child Tables completed successfully.<br/><br/>",
        )

    (
        [
            create_child2(),
            # np_contributor_summary(),
            # create_child4(),
            # create_child5(),
            create_external39(),
            create_child9(),
        ]
        >> create_views()
        >> send_notification()
    )


create_child_tables_dag = pipeline()
