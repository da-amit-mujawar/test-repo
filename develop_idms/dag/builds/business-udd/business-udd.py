from datetime import datetime
from email import header
from airflow import DAG
from operators import RedshiftOperator, DataCheckOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import os
from helpers.sqlserver import get_build_df
from helpers.common import prepare_sql_file_using_df, get_df_column
from concurrent import futures
import os, boto3, logging
import pandas as pd
from helpers.redshift import (
    executenonquery,
    get_create_table_ddl,
    copy_table_using_manifest,
)

default_args = {
    "owner": "data-axle",
    "start_date": datetime(2022, 7, 25),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    dag_id="builds-business-udd",
    description="Load Business UDD from DAP into RedShift",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get(
        "s3-axle-gold"
    )
    s3_path_temp = Variable.get("var-s3-bucket-names", deserialize_json=True).get(
        "s3-temp"
    )

    current_path = os.path.abspath(os.path.dirname(__file__))

    schema_file_path = default_args["currentpath"] + "/load_schema.csv"
    schema_df = pd.read_csv(schema_file_path)

    # Get Inactive Build
    dbid = 1450
    bdf = get_build_df(databaseid=dbid, active_flag=0)

    @task()
    def create_table(**kwargs):
        table_names = schema_df["file_type"].unique()
        for t in table_names:
            ddl = ""
            tname = f"{t.replace('.','_')}_full"
            filter_df = schema_df[schema_df["file_type"] == t]
            logging.info(f"Creating {tname}")

            ddl = get_create_table_ddl(tname, filter_df)

            executenonquery(ddl)

    @task()
    def copy_tables_using_manifest(**kwargs):
        table_names = schema_df["file_type"].unique()
        s3 = boto3.resource("s3")
        prefix = "places/udd/2022-07-29/"
        bucket = s3.Bucket(s3_path)
        all_files = bucket.objects.filter(Prefix=prefix)

        for t in table_names:
            logging.info(f"processing {t}")
            input_suffix = f"{t}.txt.gz"
            manifest_file = t + "_manifest.json"
            tname = f"{t.replace('.','_')}_full"

            copy_table_using_manifest(
                all_files, input_suffix, s3_path, s3_path_temp, manifest_file
            )

            sql = prepare_sql_file_using_df(
                sql_file=f"{current_path}/010-copy-table.sql", fetch_buildinfo=bdf
            )

            sql = sql.replace("{table_name}", tname).replace(
                "{manifest_file}", f"s3://{s3_path_temp}/{manifest_file}"
            )

            executenonquery(sql)

    @task()
    def create_maintable(**kwargs):
        # Combine Places and Contacts table into tblMain. This is place we can add simple transformation of data as needed

        sql = prepare_sql_file_using_df(
            sql_file=f"{current_path}/020-create-main-table.sql", fetch_buildinfo=bdf
        )
        executenonquery(sql)

        sql = prepare_sql_file_using_df(
            sql_file=f"{current_path}/030-insert-into-main-table.sql",
            fetch_buildinfo=bdf,
        )
        executenonquery(sql)

    @task()
    def join_business_email(**kwargs):
        email_df = get_build_df(databaseid=846, active_flag=1)

        # Bring Email from Business Email for the Same Contact ID.
        business_email_table = get_df_column(email_df, "maintable_name")

        sql = prepare_sql_file_using_df(
            sql_file=f"{current_path}/040-join-business-email.sql", fetch_buildinfo=bdf
        )
        executenonquery(sql.replace("{business-email-table}", business_email_table))

    (
        create_table()
        >> copy_tables_using_manifest()
        >> create_maintable()
        >> join_business_email()
    )


udd_dag = pipeline()
