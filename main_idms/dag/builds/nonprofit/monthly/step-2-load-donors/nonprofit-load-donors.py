from datetime import datetime
from email import header
from airflow import DAG
from operators import RedshiftOperator, DataCheckOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import os, logging
from helpers.s3 import get_files_using_wr
from helpers.nonprofit import get_donor_list_of_lists
from helpers.sqlserver import get_build_df
from helpers.common import get_df_column, prepare_sql_file_using_df
from helpers.redshift import generate_manifest, executenonquery, generate_manifest_json
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
    dag_id="builds-nonprofit-monthly-step2-load-donors",
    description="Insert Donor Non-Matches into Consumer Universe",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    current_path = os.path.abspath(os.path.dirname(__file__))
    dbid = 74

    s3_path = get_config(dbid, "s3-etl-bucket")
    s3_folder = get_config(dbid, "s3-etl-folder")
    s3_temp = get_config(dbid, "s3-temp-bucket")
    consumer_listid = get_config(dbid, "consumer-listid")

    # Get Inactive Build
    bdf = get_build_df(databaseid=dbid, active_flag=0)
    buildid = get_df_column(bdf, "build_id")
    maintable_name = get_df_column(bdf, "maintable_name")
    manifest_file = f"nonprofit_donors_manifest_{buildid}.json"
    manifest_file_trans = f"nonprofit_trans_manifest_{buildid}.json"

    # Get List of Donor Lists
    df_lol = get_donor_list_of_lists(buildid)

    # Get Drop Unique LoL
    # For Apogee, we alway retain unique
    if dbid == 1438:
        drop_unique_lol = df_lol[df_lol["dropunique"] == True].to_csv(
            columns=["listid"], index=False, header=False
        )
        drop_unique_lol = drop_unique_lol.replace("\n", ",").rstrip(",")
    else:
        drop_unique_lol = "-1"

    # Get Retain Unique LoL
    # For Apogee, we alway retain unique
    if dbid == 1438:
        retain_unique_lol = df_lol[df_lol["retainunique"] == True].to_csv(
            columns=["listid"], index=False, header=False
        )
    else:
        retain_unique_lol = df_lol.to_csv(columns=["listid"], index=False, header=False)

    retain_unique_lol = retain_unique_lol.replace("\n", ",").rstrip(",")

    @task()
    def create_manifest(**kwargs):
        logging.info(f"# of Lists {len(df_lol)}")
        folder_list = []
        for index, row in df_lol.iterrows():
            listid = row["listid"]
            listname = row["listname"]
            listaction = row["action"]
            dropunique = row["dropunique"]
            retainunique = row["retainunique"]

            # name of S3 folder where list CSV is located
            folder = {
                "bucket": s3_path,
                "prefix": f"{s3_folder}DW_Final_{dbid}_{buildid}_{listid}/",
            }
            folder_list.append(folder)

        generate_manifest(folder_list, s3_temp, manifest_file)

    @task()
    def create_table(**kwargs):
        sql = prepare_sql_file_using_df(
            sql_file=f"{current_path}/010-create-donor-table-{dbid}.sql",
            fetch_buildinfo=bdf,
        )
        executenonquery(sql)

    @task()
    def copy_tables_using_manifest(**kwargs):
        sql = prepare_sql_file_using_df(
            f"{current_path}/020-load-donor-table.sql", fetch_buildinfo=bdf
        )
        sql = sql.replace("{manifest_path}", f"s3://{s3_temp}/{manifest_file}").replace(
            "{dbid}", str(dbid)
        )
        executenonquery(sql)

    @task()
    def drop_unique_donors(**kwargs):
        logging.info(f"Unique Drop Lists {drop_unique_lol}")
        logging.info(f"Retain Unique Lists {retain_unique_lol}")

        sql = prepare_sql_file_using_df(
            f"{current_path}/030-drop-unique-donor.sql", fetch_buildinfo=bdf
        )
        sql = (
            sql.replace("{drop-unique-lol}", drop_unique_lol)
            .replace("{retain-unique-lol}", retain_unique_lol)
            .replace("{dbid}", str(dbid))
        )
        executenonquery(sql)

    @task()
    def append_nonmatch_donors_to_consumer(**kwargs):
        sql = prepare_sql_file_using_df(
            f"{current_path}/040-append-nonmatch-donor-to-consumer.sql",
            fetch_buildinfo=bdf,
        )
        sql = sql.replace("{consumer-listid}", str(consumer_listid)).replace(
            "{dbid}", str(dbid)
        )
        executenonquery(sql)

    # NP Contributor Tracking Summary
    @task()
    def np_contributor_summary():
        sql = prepare_sql_file_using_df(
            sql_file=default_args["currentpath"] + "/050*.sql",
            fetch_buildinfo=bdf,
            config_file=current_path + "/config.json",
        )
        executenonquery(sql)

    @task()
    def send_email(**kwargs):
        send_email_without_config(
            dag=dag,
            email_subject="Airflow Notice: Nonprofit Monthly Build Step 2 Load Donors Completed - {yyyy}{mm}",
            email_message="Nonprofit Monthly Build Step 2 Load Donors completed successfully.<br/><br/>",
        )

    (
        create_manifest()
        >> create_table()
        >> copy_tables_using_manifest()
        >> drop_unique_donors()
        >> append_nonmatch_donors_to_consumer()
        >> np_contributor_summary()
        >> send_email()
    )


nonprofit_dag = pipeline()
