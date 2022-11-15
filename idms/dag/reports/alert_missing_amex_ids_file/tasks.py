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
import boto3
import time


sql_output_bucket_name = 'idms-2722-internalfiles'
s3_folder_path = 's3://idms-2722-internalfiles/idms-mmdb/business/'
sql_output_key = 'idms-mmdb/business/amex_ids.txt'

def get_file_data_in_table_sql(dag, config_file, **kwargs):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    s3 = boto3.resource('s3')
    # obj = s3.Object(bucket_name='boto3', key='test.py')
    obj = s3.Object(sql_output_bucket_name, sql_output_key)
    file_created_time = str(obj.last_modified)
    print(file_created_time)
    file_created_time_new = file_created_time.split('+')[0]
    print(file_created_time_new)


    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        Declare @cPath varchar(300)
        set  @cPath  ='{s3_folder_path}'
        Declare @strMsg varchar(800)
        Declare @ncount int

        Drop table if exists ##Temp
        Create table ##temp (cfilename varchar(500), cCreateDate varchar(330), dcreateDate DateTime)

        /*   amex_ids.txt Thursday 4 pm as another job  */

        Insert into ##Temp (cfilename,cCreateDate) Values ( @cPath + 'amex_ids.txt', '{file_created_time_new}' )

        
        BEGIN TRY 
        Update ##Temp SET dcreateDate = cast (cCreateDate as datetime)
        END TRY
        BEGIN CATCH
        END CATCH

        --Select dcreateDate, cfilename  from ##temp
        
        /* Checking the file date and raise the error if the date /time is older, on Error send notification email */
        

        
        Select count(*) from ##temp where DATEDIFF(HOUR,dcreateDate, getdate()) > 9

        --Select count(*) from ##temp where DATEDIFF(DAY,dcreateDate, getdate()) > 10 --Check logic with  getdate()-1

        --If @ncount > 0

        --BEGIN 
            
        --    SET @strMsg = 'Please be notified the date created/modified for file at {s3_folder_path}amex_ids.txt are not current dates';
        --   THROW 51000, @strMsg, 1;
        
        --END
        """
        print(sql_script)
        sql_cursor.execute(sql_script)        
        row = sql_cursor.fetchone() 

        with open(config_file) as f:
            config = json.load(f)

        error_message  = config['error_message']   
        error_message_new = error_message.replace('{s3_folder_path}', s3_folder_path)

        if row[0] > 0:
            email = EmailOperator(
                mime_charset='utf-8',
                task_id='email_task',
                to=[Variable.get('var-email-on-success')],
                cc=[],
                subject=config['email_subject'],
                html_content=error_message_new,
                files=[],
                dag=dag)
            email.execute(context=kwargs)
        else:
            email = EmailOperator(
                mime_charset='utf-8',
                task_id='email_task',
                to=[Variable.get('var-email-on-success')],
                cc=[],
                subject=config['email_subject'],
                html_content=config['email_message'],
                files=[],
                dag=dag)
            email.execute(context=kwargs)



        

