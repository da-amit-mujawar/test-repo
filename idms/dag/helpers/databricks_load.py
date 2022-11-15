import base64
import json
import logging
import re
import sys
import time
from io import StringIO

import boto3
import pandas as pd
import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

ignore_list = [
    "hubspot_email_events.sql",
    "ebsprod_ra_cust_trx_line_gl_dist_all.sql",
    "subproduct_lookup.sql",
    "listbazaar_accounts.sql",
    "listbazaar_orders.sql",
    "bi_all_transactions.sql",
    "bi_lms_test.sql",
    "bi_lms_test_sg.sql",
    "salesforce_accounts.sql",
    "salesforce_contacts.sql",
    "salesforce_opportunities.sql",
    "snowflake_emaildata.sql",
    "alter_table_statements.sql",
]

## moved oess_license table to direct redshift load on 4/9/2021
load_list = [
    "oess_customer",
#    "oess_order",
#    "oess_orderitem",
    "oess_productlookup",
#    "oess_ordercancel",
    "oess_lookupitemid",
    "oess_lookupheader",
    "oess_orderfulfillmentsystems",
    "oess_fulfillmentsystem",
    "oess_ordertype",
    "oess_customeraddress",
    "ebsprod_ra_customer_trx_all",
    "ebsprod_ra_customer_trx_lines_all",
    "ebsprod_ar_collectors",
    "ebsprod_gl_code_combinations",
    "ebsprod_mtl_system_items_b",
    "ebsprod_ra_cust_trx_types_all",
    "ebsprod_ra_customers",
    "ebsprod_ra_salesreps_all",
    "ebsprod_ra_cust_trx_line_gl_dist_all",
    "ebsprod_ra_cust_trx_line_salesreps_all",
    "ebsprod_gl_accts_desc_table",
    "ebsprod_gaap_detail",
    "originreporting_accounts",
    "originreporting_eventsummarybyuser",
    "originreporting_users",
    "dotcompaymentgateway_invoice",
    "dotcompaymentgateway_order",
    "dotcompg_customer",
    "dotcompg_product",
    "dotcompg_scheduledpayment",
    "dotcompg_thinkorder",
    "dotcompg_thinkorderitem",
    "dotcompg_thinkscheduledpayment",
]


def get_bucket(bucketname):
    s3_bucket_name = Variable.get('var-s3-bucket-names', deserialize_json=True)
    return s3_bucket_name[bucketname]

def get_config(configfile):
    with open(configfile) as temp:
        return json.load(temp)

def total_time_taken(ts, te):
    # timetaken = te - ts
    hour = int((te - ts) / 3600)
    mins = ((te - ts) % 3600) / 60
    return f"is {hour:.2f} hour and {mins:.2f} min"


def test_redshift(**kwargs):
    import logging

    LOGGER = logging.getLogger("airflow.task")

    print(kwargs["redshift_conn_id"])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])
    query = f"select t.table_name\nfrom information_schema.tables t\nwhere t.table_schema = 'interna'\nand t.table_type = 'BASE TABLE'\norder by t.table_name;"
    df_tblnames = redshift_hook.get_pandas_df(sql=query)
    LOGGER.info(
        f"table_names type is {type(df_tblnames)} and table_names is {df_tblnames}"
    )
    for row in df_tblnames.itertuples(index=True, name="Pandas"):
        tbl_name = getattr(row, "table_name")
        LOGGER.info(f"table name is {tbl_name}")


def drop_table(**kwargs):
    """
    It reads the sql file created by Databricks process from S3 bucket.
    It creates tables in the Redshift interna schema using the sql query read..
    """
    import logging

    LOGGER = logging.getLogger("airflow.task")

    tsg = time.time()
    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])
    config = get_config(kwargs["config"])
    bucket = get_bucket("s-internal")
    drop_table_key = config["drop_table_key"]

    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])

    LOGGER.info(f"bucket is {bucket} and key is {drop_table_key}")
    query = read_S3_obj_to_string(bucket, drop_table_key)
    LOGGER.info(f"query for the table {drop_table_key} is {query}")
    for queryline in query.splitlines():
        LOGGER.info(f"query is {queryline}")
        try:
            redshift_hook.run(queryline, autocommit=True)
        except (Exception) as error:
            LOGGER.info(
                f"ERROR: Something went wrong while dropping table for the table {drop_table_key}  and the error is {error}"
            )
    LOGGER.info(f"Dropping of tables {drop_table_key} done.")

    teg = time.time()
    LOGGER.info(f"COMPLETE: DROPPING TABLES took {total_time_taken(tsg,teg)}")


def alter_table(**kwargs):
    """
    It reads the sql file created by Databricks process from S3 bucket.
    It creates tables in the Redshift interna schema using the sql query read..
    """
    import logging

    LOGGER = logging.getLogger("airflow.task")

    tsg = time.time()

    config = get_config(kwargs["config"])
    key = config["alter_table_key"]
    bucket = get_bucket("s3-databricks")

    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])

    LOGGER.info(f"bucket is {bucket} and key is {key}")
    query = read_S3_obj_to_string(bucket, key)
    LOGGER.info(f"query for the table {key} is {query}")
    for queryline in query.splitlines():
        LOGGER.info(f"query is {queryline}")
        try:
            redshift_hook.run(queryline, autocommit=True)
        except (Exception) as error:
            LOGGER.info(
                f"ERROR: Something went wrong while altering table for the table {key}  and the error is {error}"
            )
    LOGGER.info(f"Altering of tables {key} done.")

    teg = time.time()
    LOGGER.info(f"COMPLETE: ALTERING TABLES took {total_time_taken(tsg,teg)}")


def truncate_table(**kwargs):
    """
    It truncates all the tables one by one.
    Can improve by making it parallel if required
    """
    import logging

    LOGGER = logging.getLogger("airflow.task")

    tsg = time.time()
    print(kwargs["redshift_conn_id"])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])
    query = f"select t.table_name\nfrom information_schema.tables t\nwhere t.table_schema = 'interna'\nand t.table_type = 'BASE TABLE'\norder by t.table_name;"
    df_tblnames = redshift_hook.get_pandas_df(sql=query)
    LOGGER.info(
        f"table_names type is {type(df_tblnames)} and table_names is {df_tblnames}"
    )
    for row in df_tblnames.itertuples(index=True, name="Pandas"):
        tbl_name = getattr(row, "table_name")
        truncate_query = f"TRUNCATE TABLE interna.{tbl_name};"
        LOGGER.info(f"Truncating table {tbl_name}...")
        try:
            ts = time.time()
            redshift_hook.run(truncate_query)
            te = time.time()
            LOGGER.info(
                f"Truncating of table {tbl_name} took {total_time_taken(ts, te)} "
            )
        except (Exception) as error:
            te = time.time()
            LOGGER.info(
                f"ERROR: Something went wrong...Could not truncate table {tbl_name} and it took {total_time_taken(ts,te)} and error is {error}"
            )

    teg = time.time()
    LOGGER.info(f"COMPLETE: Truncate tables took {total_time_taken(tsg,teg)}")


def iterate_bucket_items(bucket, prefix):
    """
    Generator that iterates over all objects in a given s3 bucket and given subfolder prefix

    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    """

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket, "Prefix": prefix}
    # page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                yield item["Key"]


def read_S3_obj_to_string(bucket, key):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, key)
    query = obj.get()["Body"].read().decode("utf-8")
    return query


def create_table(**kwargs):
    """
    It reads the sql file created by Databricks process from S3 bucket.
    It creates tables in the Redshift interna schema using the sql query read..
    """
    import logging

    LOGGER = logging.getLogger("airflow.task")
    tsg = time.time()

    config = get_config(kwargs["config"])
    bucket = get_bucket("s3-databricks")
    prefix = config["prefix"]

    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])

    for key in iterate_bucket_items(bucket=bucket, prefix=prefix):
        print(f"key is {key}")
        if re.search("^.*.sql$", key):
            tablename = re.search("^.*/(.*?).sql$", key).group(1)
            print(f"tablename is {tablename}")
        else:
            continue

        if tablename in load_list and tablename not in ignore_list:
            query = read_S3_obj_to_string(bucket, key)
            LOGGER.info(f"query for the table {key} is {query}")
            try:
                ts = time.time()
                redshift_hook.run(query)
                te = time.time()
                LOGGER.info(f"Creating of table {key} took {total_time_taken(ts, te)} ")
            except (Exception) as error:
                errorstr = f"ERROR: Something went wrong while creating table for the table {key} took {total_time_taken(ts, te)}  and error is {error}"
                LOGGER.info(errorstr)
                raise Exception(errorstr)

    teg = time.time()
    LOGGER.info(f"COMPLETE: CREATE TABLE took {total_time_taken(tsg,teg)}")


def build_copy_query(bucket, key):
    table_name = key.split("/")[-1].split(".")[0]

    bucket = get_bucket("s3-databricks")
    iam_role = Variable.get("var-password-redshift-iam-role")
    copy_query = f"COPY interna.{table_name}\nFROM 's3://{bucket}/redshift-silver-tables-load/silver/{table_name}/part-'\nIAM_ROLE '{iam_role}'\nFORMAT AS PARQUET"

    return copy_query


def load_table(**kwargs):
    """
    It loads data into the tables through COPY command.
    """
    import logging

    LOGGER = logging.getLogger("airflow.task")

    config = get_config(kwargs["config"])
    bucket = get_bucket("s3-databricks")
    prefix = config["prefix"]

    tsg = time.time()
    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])

    for key in iterate_bucket_items(bucket=bucket, prefix=prefix):
        print(f"key is {key}")
        if re.search("^.*.sql$", key):
            tablename = re.search("^.*/(.*?).sql$", key).group(1)
            print(f"tablename is {tablename}")
        else:
            continue

        if tablename in load_list and tablename not in ignore_list:
            LOGGER.info(f"bucket is {bucket} and key is {key}")
            ts = time.time()
            copy_query = build_copy_query(bucket, key)
            LOGGER.info(copy_query)
            try:
                redshift_hook.run(copy_query)
                te = time.time()
                LOGGER.info(f"Loading of table {key} took {total_time_taken(ts, te)} ")
            except (Exception) as error:
                te = time.time()
                errorstr = f"ERROR: Something went wrong while loading data for the table {key} took {total_time_taken(ts, te)} and error is {error}"
                LOGGER.info(errorstr)
                raise Exception(errorstr)

    teg = time.time()
    LOGGER.info(f"COMPLETE: LOAD TABLES took {total_time_taken(tsg,teg)}")


def validate_table(**kwargs):
    """
    It returns the row count for all the tables.
    """
    import logging

    LOGGER = logging.getLogger("airflow.task")

    redshift_hook = PostgresHook(postgres_conn_id=kwargs["redshift_conn_id"])
    config = get_config(kwargs["config"])

    bucket = get_bucket("s3-databricks")
    count_file_key = config["count_file_key"]
    prefix = config["prefix"]

    try:
        count_file_csv_string = read_S3_obj_to_string(bucket, count_file_key)
        table_count = pd.read_csv(StringIO(count_file_csv_string))
    except (Exception) as error:
        print(
            f"ERROR: not able to read the file {bucket}/{count_file_key} and the error is {error}"
        )

    for row_index, row in table_count.iterrows():
        for colname in table_count.columns:
            tbl_name = row["tablename"]
            db_count = int(row["count"])

        count_query = f"SELECT COUNT(*) FROM interna.{tbl_name};"
        LOGGER.info(f"Getting count for table {tbl_name}...")
        redshift_count = 0
        try:
            redshift_count = redshift_hook.get_first(count_query)[0]
            LOGGER.info(f"Count of table {tbl_name} is {redshift_count}")
        except Exception as e:
            errorstr = f"ERROR: Count Validate failed for table {tbl_name} and error is {str(e)}"
            LOGGER.info(errorstr)
            raise Exception(errorstr)

        if (redshift_count != db_count):
            errorstr = f"ERROR: Count mismatch for table {tbl_name} RedShift Count {redshift_count} DataBricks Count {db_count}"
            LOGGER.info(errorstr)
            raise Exception(errorstr)


def call_databricks_jobs(DOMAIN, TOKEN, JOB):
    bucket = get_bucket("s3-databricks")
    response = requests.post(
        "https://%s/api/2.0/jobs/run-now" % (DOMAIN),
        headers={
            "Authorization": b"Basic " + base64.standard_b64encode(b"token:" + TOKEN)
        },
        json={"job_id": JOB, "timeout_seconds": "3600", "notebook_params": {"bucket": bucket}},
    )

    print(f"response structure is: {response}")
    if response.status_code == 200:
        print(response.json())
        return response.json()
    else:
        print("Error launching cluster: %s" % (response.json()))


def schema_dclone_dbricks_tables(**kwargs):
    token = kwargs["token"].encode("utf-8")
    jobname = kwargs["job"]
    config = get_config(kwargs["config"])
    domain = Variable.get("var-databricks-url")
    jobid = config[jobname]
    runid = (call_databricks_jobs(domain, token, jobid))["run_id"]
    task_instance = kwargs["ti"]
    print(task_instance)
    task_instance.xcom_push("run_id", runid)
