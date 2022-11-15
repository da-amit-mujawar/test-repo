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
import logging


def get_engagers_counts_sql(dag, config_file,**kwargs):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with open(config_file) as f:
        config = json.load(f)

    email_message = config ['email_message']

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
            Select CASE WHEN isApplied =1 THEN 'Yes' ELSE 'No' end as [Status], 
            databaseid , 
            cDatabaseName, 
            BuildID, 
            cTablename 
            from Tempdata.dbo.eng_b2b_tables 
            Order by 1,2 desc
        """
        logging.info(f"script : {sql_script}")
        # print(sql_script)
        sql_cursor.execute(sql_script)        
        records = sql_cursor.fetchall()
        strResult = "<br /><br />Status   |DatabaseID|DatabaseName|BuildID|MainTableName"  +" <br />"
        for row in records:
            strResult = strResult \
                        + row[0] + "    |" \
                        + str(row[1] )+ "|"\
                        + row[2] + "|"\
                        + str(row[3]) + "|" \
                        + row[4] \
                        + "<br /> "

        email_message = email_message.replace('<<StdOut>>', strResult)

        # send email with the result set
        email = EmailOperator(
        mime_charset='utf-8',
        task_id='email_task',
        to=[Variable.get('var-email-dworders')],
        cc=[Variable.get('var-email-on-success')],
        subject=config['email_subject'],
        html_content=email_message,
        files=[],
        dag=dag)
        email.execute(context=kwargs)
