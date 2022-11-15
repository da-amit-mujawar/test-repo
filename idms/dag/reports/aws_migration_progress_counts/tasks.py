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


def get_aws_migration_counts_sql(dag, config_file,**kwargs):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with open(config_file) as f:
        config = json.load(f)

    AccountingDivisionCode = config['AccountingDivisionCode']
    email_message   = config['email_message']
    StartDate       = config['StartDate']
    ExcludeDbs      = config["ExcludeDbs"]

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
          DROP TABLE IF EXISTS #Temp
        
            select count(distinct MasterLoLID)as NumberofSources
             ,count(distinct DatabaseID)as NumberofDatabases
            ,max(aws.[Number of Sources Migrated to AWS]) as NumberofSourcesMigratedtoAWS
            ,max(aws.[Number of Databases Migrated to AWS]) as NumberofDatabasesMigratedtoAWS
            ,max(aws.[Number of Sources Migrated to AWS]) * 100 / count(distinct MasterLoLID)as PercentageCompleted
            into #Temp
            from dw_admin.dbo.tblBuildLoL a
            inner join dw_admin.dbo.tblBuild d
            on d.id = a.BuildID
            inner join dw_admin.dbo.tblDatabase e
            on e.id = d.DatabaseID
            cross join (
            select count(distinct MasterLoLID) [Number of Sources Migrated to AWS], count(distinct DatabaseID) [Number of Databases Migrated to AWS]
            --select e.id, e.cDatabaseName,a.*
            from dw_admin.dbo.tblBuildLoL a
            inner join dw_admin.dbo.tblBuild d
            on d.id = a.BuildID
            inner join dw_admin.dbo.tblDatabase e
            on e.id = d.DatabaseID
            where a.dCreatedDate >= '<<StartDate>>'
            and e.LK_AccountingDivisionCode = '<<AccountingDivisionCode>>' -- B2C Divsion 
            and e.DivisionID > 177
            and e.id not in (<<ExcludeDbs>>) -- Exclude DBs we built using VC
            ) aws
            where a.dCreatedDate between GETDATE() - 365 AND '<<StartDate>>'
            and e.LK_AccountingDivisionCode = '<<AccountingDivisionCode>>' -- B2C Divsion 
            and e.DivisionID > 177
            and e.id not in (<<ExcludeDbs>>) -- Exclude DBs we built using VC
        
        
            --Select * from #Temp
            SELECT 
              '<p>' +
              'Number of Sources in Past 1 Year: ' + Cast(NumberofSources as varchar)  + '<br />' +
              'Number of Databases in Past 1 Year: ' + Cast(NumberofDatabases as varchar)  + '<br />' +
              'Number of Sources Migrated to AWS: ' + Cast(NumberofSourcesMigratedtoAWS as varchar)  + '<br />' +
              'Number of Databases Migrated to AWS: ' + Cast(NumberofDatabasesMigratedtoAWS as varchar)  + '<br />' +
              '% Completed: ' + Cast(PercentageCompleted as varchar)  + '<br />'
              +'</p>'
              From #Temp
 
        """

        sql_script = sql_script.replace('<<AccountingDivisionCode>>', AccountingDivisionCode)
        sql_script = sql_script.replace('<<StartDate>>', StartDate)
        sql_script = sql_script.replace('<<ExcludeDbs>>', ExcludeDbs)

        logging.info(f"script : {sql_script}")
        print(sql_script)
        sql_cursor.execute(sql_script)        
        records = sql_cursor.fetchall()
        strResult = ''
        for row in records:
            strResult = strResult + row[0] + "\n"

        email_message = email_message.replace('<<StdOut>>', strResult)

        #send email witth the result
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




        

