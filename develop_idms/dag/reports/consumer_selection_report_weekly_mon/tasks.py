from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp
import pandas as pd
from pandas.io import sql as psql
from helpers.s3 import save_dataframe


sql_output_bucket_name = 'develop_idms-2722-internalfiles'
sql_output_folder = 'Reports/RoyaltyUsage/'


def generate_report(mode, report_name, report_path):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    sql_output_key = sql_output_folder + report_name

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        """
        print(sql_script)
        sql_cursor.execute(sql_script)
        sql_script = f"""
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        time_stamp = datetime.today().strftime('%Y%m%d')
        s3_key = sql_output_key.replace('{yyyymmdd}', time_stamp)
        save_dataframe(sql_output_bucket_name, s3_key, orders_df, '|')
        orders_df.to_csv(report_path, '|', index=False,)
