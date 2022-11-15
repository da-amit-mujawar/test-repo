from datetime import datetime, timedelta
from airflow.models import Variable
from helpers.send_email import send_email
from helpers.s3 import read_file
from helpers.databricks import start_databricks_job
from helpers.redshift import *
import json
import glob
import pandas as pd
import re


def get_file_content(filename):
    fd = open(filename, "r")
    content = fd.read()
    fd.close()
    return content


def get_runtime_arg(item, **kwargs):
    if len(kwargs["dag_run"].conf) >= 1:
        return kwargs["dag_run"].conf.get(item)
    else:
        raise Exception("Runtime args is missing. Trigger DAG with required arguments.")


def prepare_sql_file(sql_file, config_file=None):
    current_date = datetime.now()

    fd = open(sql_file, "r")
    sql = fd.read()
    fd.close()

    sql = sql.replace("{iam}", Variable.get("var-password-redshift-iam-role"))
    sql = sql.replace("{kmskey}", Variable.get("var-password-kms-key"))

    if config_file is not None:
        with open(config_file) as f:
            config = json.load(f)

        for item in config:
            sql = sql.replace("{" + item + "}", config[item])

    s3_bucket_name = Variable.get("var-s3-bucket-names", deserialize_json=True)

    for bucket in s3_bucket_name:
        sql = sql.replace("{" + bucket + "}", s3_bucket_name[bucket])

    sql = sql.replace("{dd}", current_date.strftime("%d"))
    sql = sql.replace("{mm}", current_date.strftime("%m"))
    sql = sql.replace("{yyyy}", current_date.strftime("%Y"))
    today = datetime.now()
    yesterday = datetime.now() - timedelta(1)
    sql = sql.replace("{yesterday}", datetime.strftime(yesterday, "%Y%m%d"))
    sql = sql.replace("{today}", datetime.strftime(today, "%Y%m%d"))
    sql = sql.replace(
        "{emailoversight-prefix}", Variable.get("var-emailoversight-prefix")
    )
    return sql


def get_df_column(df, column_name):
    if df is not None:
        if column_name in df:
            return df[column_name][0]
        else:
            return "0"
    else:
        return "0"


def prepare_sql_file_using_df(sql_file, fetch_buildinfo, config_file=None):
    current_date = datetime.now()
    final_sql = " "
    for sql_file in sorted(glob.glob(sql_file)):
        fd = open(sql_file, "r")
        sql = fd.read()
        fd.close()

        sql = sql.replace("{iam}", Variable.get("var-password-redshift-iam-role"))
        sql = sql.replace("{kmskey}", Variable.get("var-password-kms-key"))

        if config_file is not None:
            with open(config_file) as f:
                config = json.load(f)

            for item in config:
                sql = sql.replace("{" + item + "}", config[item])

        s3_bucket_name = Variable.get("var-s3-bucket-names", deserialize_json=True)

        for bucket in s3_bucket_name:
            sql = sql.replace("{" + bucket + "}", s3_bucket_name[bucket])

        sql = sql.replace("{dd}", current_date.strftime("%d"))
        sql = sql.replace("{mm}", current_date.strftime("%m"))
        sql = sql.replace("{yyyy}", current_date.strftime("%Y"))
        today = datetime.now()
        yesterday = datetime.now() - timedelta(1)
        sql = sql.replace("{yesterday}", datetime.strftime(yesterday, "%Y%m%d"))
        sql = sql.replace("{today}", datetime.strftime(today, "%Y%m%d"))
        sql = sql.replace(
            "{emailoversight-prefix}", Variable.get("var-emailoversight-prefix")
        )

        if fetch_buildinfo is not None:
            for key, value in fetch_buildinfo.items():
                if len(value) > 0:
                    sql = sql.replace("{" + key + "}", str(value[0]))

        final_sql = final_sql + sql

    return final_sql


def remove_bad_chars(listname):
    listname = re.sub(r'[\\/*?:"<>|]', "", listname)
    return listname
