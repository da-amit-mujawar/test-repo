import os
import subprocess
from datetime import datetime

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 3, 26),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
    "email_on_retry": False,
}

dag = DAG(
    "dynamic-ddls",
    default_args=default_args,
    description="Dynamic generation of DDL for Redshift tables",
    schedule_interval="@once",
    max_active_runs=1,
)


def fetch_header(bucket, file, suffix):
    cat_val = "cat"
    if "gz" in suffix:
        cat_val = "zcat"
    filename = "s3://" + bucket.name + "/" + file.key
    cmd = "aws s3 cp " + filename + " - | " + cat_val + " | head -n 1"
    ps = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    header_string = ps.communicate()[0].decode("utf-8").split("\n", 1)[0].strip()
    print("header for file: ", file.key, " is ", header_string)
    return header_string


def build_ddl(h, filter_df, keywords_list, ddl):
    f_df = filter_df[filter_df["column_name"] == h]
    # f_df.show()
    ctype = (f_df["column_type"]).to_string(index=False).strip().lower()
    clength = str((f_df["column_length"]).to_string(index=False).strip()).split(".")[0]
    encode = (f_df["encoding"]).to_string(index=False).strip()
    pkey = (f_df["primary_key"]).to_string(index=False).strip()
    notnull = (f_df["not_null"]).to_string(index=False).strip()
    sortkey = (f_df["sort_key"]).to_string(index=False).strip()
    defaults = (f_df["default"]).to_string(index=False).strip()

    h = '"' + h + '"'

    if ctype == "varchar":
        ddl += f"""
{h} {ctype}({clength})"""
        if defaults != "No":
            ddl += " default '" + defaults + "'"
    else:
        ddl += f"""
{h} {ctype}"""
        if defaults != "No":
            ddl += " default " + defaults
    if notnull == "y":
        ddl += " NOT NULL"
    if sortkey == "y":
        ddl += " SORTKEY"
    if pkey == "y":
        ddl += " PRIMARY KEY"
    if encode != "NA":
        ddl += f""" ENCODE {encode}"""
    ddl += ","
    return ddl


def create_without_header(fname, filter_df, keywords_list):
    ddl = f"""DROP TABLE IF EXISTS core_bf.{fname};"""
    ddl = (
        ddl
        + f"""
CREATE TABLE core_bf.{fname}
( """
    )
    count = 0
    for h in filter_df.values:
        count += 1
        ddl = build_ddl(h[1], filter_df, keywords_list, ddl)

    print("file type ", fname, " has ", count, " columns")
    return ddl


def create_with_header(
    bucket, fname, prefix, cdate, suffix, filter_df, keywords_list, delimiter
):

    for file in bucket.objects.filter(Prefix=prefix + str(cdate)):
        print("file name: ", file)
        if file.key.endswith(suffix) and fname in file.key:
            ddl = f"""DROP TABLE IF EXISTS core_bf.{fname};"""
            ddl = (
                ddl
                + f"""
CREATE TABLE core_bf.{fname}
( """
            )

            header_str = fetch_header(bucket, file, suffix)
            header_str = header_str.replace(".", "_")
            header_list = header_str.split(delimiter)

            # filter_df.display()
            count = 0
            for h in header_list:
                if h in filter_df.values:
                    count += 1
                    ddl = build_ddl(h, filter_df, keywords_list, ddl)

            print("file type ", fname, " has ", count, " columns")
            print("Total columns in file header for ", fname, " is ", len(header_list))
            return ddl


def create_ddls(
    tables_list, bucket, header, refer_df, suffix, prefix, output_path, delimiter, dist
):
    keywords_list = [
        "AES128",
        "AES256",
        "ALL",
        "ALLOWOVERWRITE",
        "ANALYSE",
        "ANALYZE",
        "AND",
        "ANY",
        "ARRAY",
        "AS",
        "ASC",
        "AUTHORIZATION",
        "AZ64",
        "BACKUP",
        "BETWEEN",
        "BINARY",
        "BLANKSASNULL",
        "BOTH",
        "BYTEDICT",
        "BZIP2",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "CONSTRAINT",
        "CREATE",
        "CREDENTIALS",
        "CROSS",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "CURRENT_USER_ID",
        "DEFAULT",
        "DEFERRABLE",
        "DEFLATE",
        "DEFRAG",
        "DELTA",
        "DELTA32K",
        "DESC",
        "DISABLE",
        "DISTINCT",
        "DO",
        "ELSE",
        "EMPTYASNULL",
        "ENABLE",
        "ENCODE",
        "ENCRYPT",
        "ENCRYPTION",
        "END",
        "EXCEPT",
        "EXPLICIT",
        "FALSE",
        "FOR",
        "FOREIGN",
        "FREEZE",
        "FROM",
        "FULL",
        "GLOBALDICT256",
        "GLOBALDICT64K",
        "GRANT",
        "GROUP",
        "GZIP",
        "HAVING",
        "IDENTITY",
        "IGNORE",
        "ILIKE",
        "IN",
        "INITIALLY",
        "INNER",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "LANGUAGE",
        "LEADING",
        "LEFT",
        "LIKE",
        "LIMIT",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "LUN",
        "LUNS",
        "LZO",
        "LZOP",
        "MINUS",
        "MOSTLY16",
        "MOSTLY32",
        "MOSTLY8",
        "NATURAL",
        "NEW",
        "NOT",
        "NOTNULL",
        "NULL",
        "NULLS",
        "OFF",
        "OFFLINE",
        "OFFSET",
        "OID",
        "OLD",
        "ON",
        "ONLY",
        "OPEN",
        "OR",
        "ORDER",
        "OUTER",
        "OVERLAPS",
        "PARALLEL",
        "PARTITION",
        "PERCENT",
        "PERMISSIONS",
        "PLACING",
        "PRIMARY",
        "RAW",
        "READRATIO",
        "RECOVER",
        "REFERENCES",
        "RESPECT",
        "REJECTLOG",
        "RESORT",
        "RESTORE",
        "RIGHT",
        "SELECT",
        "SESSION_USER",
        "SIMILAR",
        "SNAPSHOT",
        "SOME",
        "SYSDATE",
        "SYSTEM",
        "TABLE",
        "TAG",
        "TDES",
        "TEXT255",
        "TEXT32K",
        "THEN",
        "TIMESTAMP",
        "TO",
        "TOP",
        "TRAILING",
        "TRUE",
        "TRUNCATECOLUMNS",
        "UNION",
        "UNIQUE",
        "USER",
        "USING",
        "VERBOSE",
        "WALLET",
        "WHEN",
        "WHERE",
        "WITH",
        "WITHOUT",
    ]
    cdate = datetime.today().strftime("%Y%m%d")
    cdate = 20210312
    print(str(cdate))
    for fname in tables_list:
        print("in for ", fname)
        ddl = ""
        filter_df = refer_df[refer_df["file_type"] == fname]
        if header == "yes":
            ddl = create_with_header(
                bucket,
                fname,
                prefix,
                cdate,
                suffix,
                filter_df,
                keywords_list,
                delimiter,
            )
        else:
            ddl = create_without_header(fname, filter_df, keywords_list)

        ddl = ddl[:-1]
        ddl = (
            ddl
            + f"""
)
diststyle {dist}
;"""
        )
        outfile_name = default_args["currentpath"] + output_path + fname + "_ddl.sql"
        with open(outfile_name, "w") as text_file:
            text_file.write(ddl)

        infile_name = default_args["currentpath"] + output_path + fname + "_ddl.sql"
        with open(infile_name, "r") as text_file:
            print("Written on local in file " + infile_name + " " + text_file.read())


def generate_ddl_function(**kwargs):

    # Get arguments
    input_bucket = kwargs["dag_run"].conf.get("input-bucket")
    file_types = kwargs["dag_run"].conf.get("file-types")
    header = kwargs["dag_run"].conf.get("header")
    delimiter = kwargs["dag_run"].conf.get("delimiter")
    suffix = kwargs["dag_run"].conf.get("suffix")
    prefix = kwargs["dag_run"].conf.get("prefix")
    output_path = kwargs["dag_run"].conf.get("output-path")
    refer_bucket = kwargs["dag_run"].conf.get("refer-bucket")
    refer_filename = kwargs["dag_run"].conf.get("refer-filename")
    dist = kwargs["dag_run"].conf.get("dist")

    if None in (
        input_bucket,
        file_types,
        header,
        delimiter,
        suffix,
        prefix,
        output_path,
        refer_bucket,
        refer_filename,
        dist,
    ):
        raise ValueError(
            "Please pass all the 10 arguments input-bucket, file-types, header, delimiter, suffix, prefix, output-path, refer-bucket, refer-filename, dist"
        )

    # Read CSV file for schema
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=refer_bucket, Key=refer_filename)
    refer_df = pd.read_csv(obj["Body"])
    # refer_file_path=default_args['currentpath'] + '/refer_full.csv'
    # refer_df = pd.read_csv(refer_file_path)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(input_bucket)

    tables = file_types
    tables_list = tables.split(",")

    create_ddls(
        tables_list,
        bucket,
        header,
        refer_df,
        suffix,
        prefix,
        output_path,
        delimiter,
        dist,
    )


generate_ddl = PythonOperator(
    task_id="generate_ddl_task",
    python_callable=generate_ddl_function,
    provide_context=True,
    dag=dag,
)

generate_ddl
