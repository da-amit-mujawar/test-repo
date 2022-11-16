from datetime import datetime
from email import header
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
import os, logging
from helpers.donorbase import get_donor_list_of_lists
from helpers.sqlserver import get_build_df
from helpers.redshift import generate_manifest, executenonquery, generate_manifest_json
from helpers.common import prepare_sql_file_using_df, get_df_column
from helpers.nonprofit import get_config
from helpers.s3 import get_files_using_wr

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
    dag_id="builds-donorbase-load-donors",
    description="Donorbase RedShift Database Build Pipeline",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    current_path = os.path.abspath(os.path.dirname(__file__))

    # Get Inactive Build
    databaseid = 1438
    fetch_buildinfo_df = get_build_df(databaseid=databaseid, active_flag=0)
    buildid = get_df_column(fetch_buildinfo_df, "build_id")
    s3_temp = get_config(databaseid, "s3-temp-bucket")
    manifest_file = "donorbase_manifest.json"

    # Get List of Donor Lists
    df_lol = get_donor_list_of_lists(buildid)

    # Get Drop Unique LoL
    drop_unique_lol = df_lol[df_lol["dropunique"] == True].to_csv(
        columns=["listid"], index=False, header=False
    )
    drop_unique_lol = drop_unique_lol.replace("\n", ",").rstrip(",")

    # Get Retain Unique LoL
    retain_unique_lol = df_lol[df_lol["retainunique"] == True].to_csv(
        columns=["listid"], index=False, header=False
    )
    retain_unique_lol = retain_unique_lol.replace("\n", ",").rstrip(",")

    @task()
    def create_manifest(**kwargs):
        logging.info(f"# of Lists {len(df_lol)}")
        folder_list = []
        etl_bucket = get_config(databaseid, "s3-etl-bucket")
        etl_folder = get_config(databaseid, "s3-etl-folder")

        for index, row in df_lol.iterrows():
            listid = row["listid"]

            # name of S3 folder where list CSV is located
            folder = {
                "bucket": etl_bucket,
                "prefix": f"{etl_folder}DW_Final_{databaseid}_{buildid}_{listid}/",
            }
            folder_list.append(folder)

        generate_manifest(folder_list, s3_temp, manifest_file)

    @task()
    def create_table(**kwargs):
        sql = prepare_sql_file_using_df(
            f"{current_path}/010-create-donor-table.sql",
            fetch_buildinfo=fetch_buildinfo_df,
        )

        executenonquery(sql)

    @task()
    def copy_tables_using_manifest(**kwargs):
        sql = prepare_sql_file_using_df(
            f"{current_path}/020-load-donor-table.sql",
            fetch_buildinfo=fetch_buildinfo_df,
        )

        s3_url = f"s3://{s3_temp}/{manifest_file}"
        logging.info(f"Reading from S3 {s3_url}")
        sql = sql.replace("{manifest_path}", s3_url)

        executenonquery(sql)

    @task()
    def drop_unique_donors(**kwargs):
        logging.info(f"Unique Drop Lists {drop_unique_lol}")
        logging.info(f"Retain Unique Lists {retain_unique_lol}")

        sql = prepare_sql_file_using_df(
            f"{current_path}/030-drop-unique-donor.sql",
            fetch_buildinfo=fetch_buildinfo_df,
        )

        sql = sql.replace("{drop-unique-lol}", drop_unique_lol).replace(
            "{retain-unique-lol}", retain_unique_lol
        )

        executenonquery(sql)

    @task()
    def append_nonmatch_donors_to_consumer(**kwargs):
        sql = prepare_sql_file_using_df(
            f"{current_path}/040-append-nonmatch-donor-to-consumer.sql",
            fetch_buildinfo=fetch_buildinfo_df,
        )

        executenonquery(sql)

    @task()
    def load_transactions(**kwargs):
        manifest_file_trans = f"nonprofit_trans_manifest_{buildid}.json"
        export_bucket = get_config(databaseid, "s3-silver-bucket")
        export_folder = get_config(databaseid, "s3-silver-folder")
        s3_manifest_url = f"s3://{s3_temp}/{manifest_file_trans}"
        s3_data_url = f"s3://{export_bucket}{export_folder}*/*.gz"
        logging.info(f"Reading from manifest file {s3_manifest_url}")
        logging.info(f"Reading from data file {s3_data_url}")

        files = get_files_using_wr(s3_data_url)
        generate_manifest_json(s3_temp, manifest_file_trans, files)

        sql = prepare_sql_file_using_df(
            f"{current_path}/050*.sql",
            fetch_buildinfo=fetch_buildinfo_df,
        )
        sql = sql.replace("{manifest_path}", s3_manifest_url)
        executenonquery(sql)

    (
        create_manifest()
        >> create_table()
        >> copy_tables_using_manifest()
        >> drop_unique_donors()
        >> append_nonmatch_donors_to_consumer()
        >> load_transactions()
    )


donorbase_dag = pipeline()
