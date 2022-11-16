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

def setup_b2c_db_in_aws_sql(dag, config_file,**kwargs):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    with open(config_file) as f:
        config = json.load(f)

    databaseid = Variable.get('var-support', deserialize_json=True, default_var=0)['etl_awsconfig_db']
    S3_ListFileUploadedPath =  's3://{s3-listcoversion}' + Variable.get('var-support', deserialize_json=True, default_var=0)['etl_awsconfig_ListFileUploadedPath']
    S3_ListReadyToLoadPath = 's3:' +  Variable.get('var-support', deserialize_json=True, default_var=0)['etl_awsconfig_ListReadyToLoadPath']
    s3_bucket_name = Variable.get('var-s3-bucket-names', deserialize_json=True)
    username = config['supportusername'].upper()

    email_message = config['email_message']
    excludelist = list(map(int,config['ExcludeDbs'].upper().split(',')))


    #log the details
    logging.info(f"ETL AWS configure for the db:  {databaseid}")
    logging.info(f"Exclude list:")
    logging.info(excludelist)

    if databaseid in excludelist :
        logging.info(f"ETL AWS configure for the db is skipped for : {databaseid}.")
        return
    else:
        logging.info(f"ETL AWS configure  - Continue : {databaseid}.")

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
            DECLARE @DBID int, @ListID int
            
            SET @DBID = <<DatabaseID>>
            SELECT TOP 1 @ListID = ID FROM dw_admin.dbo.tblMasterLoL WHERE DatabaseID = @DBID AND iIsActive = 1
            
            IF @ListID IS NULL
                --PRINT 'List ID is NULL'
                THROW 51000, 'List ID not found.', 1;
            
            UPDATE TOP (1) dw_admin.dbo.tblDatabase
               SET cListFileUploadedPath = '<<S3_ListFileUploadedPath>>',
                   cListReadyToLoadPath = '<<S3_ListReadyToLoadPath>>',
                   dModifiedDate = GETDATE(), cModifiedBy = '<<username>>'
            WHERE ID = @DBID
            
            IF NOT EXISTS(SELECT * FROM dw_admin.dbo.tblConfiguration WHERE cItem = 'AWSETL' AND DatabaseID = @DBID)
                INSERT INTO dw_admin.dbo.tblConfiguration (DivisionID, DatabaseID, cDescription,  cItem,     cValue, mValue, iValue, iIsActive, iIsEncrypted, dCreatedDate, cCreatedBy)
                                  VALUES (    188,       @DBID, 'LoadProcess', 'AWSETL', CAST(@ListID as varchar),     '',      0,         1,            0,    GETDATE(), '<<username>>')  
            
            -- For multi-list db, hardcode the values
            /*
            INSERT INTO dw_admin.dbo.tblConfiguration (DivisionID, DatabaseID, cDescription,  cItem,     cValue, mValue, iValue, iIsActive, iIsEncrypted, dCreatedDate, cCreatedBy)
                                  VALUES (       188,       @DBID, 'LoadProcess', 'AWSETL', '15712,15494,15495,15497,15496,15493',     '',      0,         1,            0,    GETDATE(), '<<username>>')  
            */                
            
            IF NOT EXISTS(SELECT * FROM dw_admin.dbo.tblConfiguration WHERE cItem = 'SKIPAUDITREPORT' AND DatabaseID = @DBID)
                INSERT INTO dw_admin.dbo.tblConfiguration (DivisionID, DatabaseID, cDescription,  cItem,           cValue, mValue, iValue, iIsActive, iIsEncrypted, dCreatedDate, cCreatedBy)
                                      VALUES (   188,       @DBID, 'LoadProcess', 'SKIPAUDITREPORT', '1',     '',      0,         1,            0,    GETDATE(), '<<username>>')  


        """

        sql_script = sql_script.replace('<<DatabaseID>>', str(databaseid))
        sql_script = sql_script.replace('<<S3_ListFileUploadedPath>>', S3_ListFileUploadedPath)
        sql_script = sql_script.replace('<<S3_ListReadyToLoadPath>>', S3_ListReadyToLoadPath)
        sql_script = sql_script.replace('<<username>>', username)
        for bucket in s3_bucket_name:
            sql_script = sql_script.replace('{' + bucket + '}', s3_bucket_name[bucket])

        logging.info(f"script : {sql_script}")
        print(sql_script)
        sql_cursor.execute(sql_script)

        # Get the Database name

        sqlresult = get_top1_record (["cDatabaseName", "cAdministratorEmail"], "dw_admin.dbo.tblDatabase", " WHERE ID = "+ str(databaseid) )
        logging.info(f"Top 1 record values: {sqlresult}")
        strdatabasename = sqlresult["cDatabaseName"]
        stradminemail = sqlresult["cAdministratorEmail"]

    email_message = email_message.replace('<<StdOut>>', strdatabasename)
    email_message = email_message.replace('<<databaseid>>', str(databaseid))

    #send email witth the result
    email = EmailOperator(
    mime_charset='utf-8',
    task_id='email_task',
    to=stradminemail,
    cc=[Variable.get('var-email-dworders')],
    subject=config['email_subject'],
    html_content=email_message,
    files=[],
    dag=dag)
    email.execute(context=kwargs)




        

