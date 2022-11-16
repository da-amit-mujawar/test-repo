from datetime import datetime
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
sql_output_key = 'internaltransfer/Temp/1008_ShippingReport_{start_date}_To_{end_date}.csv'


def generate_report(start_date, end_date, report_path):
    # 1. start_date & end_date should be in 'YYYYMMDD' format that is  '20200820'.
    # 2. For running report between yesterday and today, they both can be set to ''.
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    output_key = sql_output_key.replace('{start_date}', start_date)
    output_key = output_key.replace('{end_date}', end_date)
    daily_monthly = 'D'  # VC_JOB_VARIABLE

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""  
        DECLARE @tcBeginDate  VARCHAR(8) = '{start_date}';
        DECLARE @tcEndDate  VARCHAR(8) = '{end_date}';        
        DECLARE @iDatabaseId  INT = 1008;

        DROP TABLE IF EXISTS ##Report1;

        SELECT DISTINCT(a.ID) [Order ID],
            a.cDescription [Order Description],
            a.iProvidedCount [Quantity],
            a.cBrokerPONo [Broker PO NO],
            DM.cCompany Mailer,
            c.cDatabaseName [Database Name],
            a.dShipDateShipped,
            LOL.cListName,
            ltrim(rtrim(a.cDecoyKey)) [Decoy Key]
        INTO ##AmeriMarkResult
        FROM Dw_admin.dbo.tblorder a (nolock)
        INNER JOIN Dw_admin.dbo.tblBuild b (nolock) ON a.buildid = b.id
        INNER JOIN Dw_admin.dbo.tbldatabase c (nolock) ON c.id = b.databaseid
        INNER JOIN Dw_admin.dbo.tbldivision d (nolock) ON d.id = c.DivisionID
        INNER JOIN Dw_admin.dbo.tblDivisionMailer DM (nolock) ON a.DivisionMailerId = DM.Id
        INNER JOIN Dw_admin.dbo.tblsegment Seg (nolock) ON A.id=Seg.Orderid
        INNER JOIN Dw_admin.dbo.tblsegmentlist SegL (nolock) ON SegL.SegmentId=Seg.id
        INNER JOIN Dw_admin.dbo.tblMasterLoL LOL (NOLOCK) ON SegL.MasterLolid=LOL.id
        WHERE c.ID = @iDatabaseId
        AND a.dShipDateShipped BETWEEN @tcBeginDate AND @tcEndDate
        ORDER BY a.id


        INSERT into ##AmeriMarkResult 
                    ([Order ID],[Order Description],[Quantity],
                    [Broker PO NO],Mailer,[Database Name], dShipDateShipped, 
                    cListName, [Decoy Key])
                    select distinct(a.ID) [Order ID],a.cDescription [Order Description],a.iProvidedCount [Quantity],
                    a.cBrokerPONo [Broker PO NO],DM.cCompany Mailer,c.cDatabaseName [Database Name],  a.dShipDateShipped, 
                    CASE WHEN LOL.cListName is NULL then '' ELSE LOL.cListName END , ltrim(rtrim(a.cDecoyKey)) [Decoy Key]
                    from Dw_admin.dbo.tblorder a (nolock)
                    inner join Dw_admin.dbo.tblBuild b (nolock) on a.buildid = b.id 
                    inner join Dw_admin.dbo.tbldatabase c (nolock) on c.id = b.databaseid 
                    inner join Dw_admin.dbo.tbldivision d (nolock) on d.id = c.DivisionID 
                    inner join Dw_admin.dbo.tblDivisionMailer DM (nolock) on a.DivisionMailerId = DM.Id 
                    inner join Dw_admin.dbo.tblsegment Seg (nolock) on A.id=Seg.Orderid
                    LEFT join Dw_admin.dbo.tblsegmentlist SegL (nolock) ON  SegL.SegmentId=Seg.id
                    LEFT join Dw_admin.dbo.tblMasterLoL LOL (NOLOCK) ON SegL.MasterLolid=LOL.id
                    where c.ID = @iDatabaseId
                    and a.dShipDateShipped between @tcBeginDate and @tcEndDate
                    AND a.id  NOT IN (Select distinct [Order ID] from  ##AmeriMarkResult)
                    ORDER BY a.id              
        """
        print(sql_script)
        sql_cursor.execute(sql_script)
        sql_script = f"""
        SELECT CAST ([Order ID] AS VARCHAR(100)) [Order ID],
        CAST([Order Description] AS VARCHAR(100)) [Order Description],
        CAST([Quantity] AS VARCHAR(100)) [Quantity],
        CAST([Broker PO NO] AS VARCHAR(100)) [Broker PO NO],
        CAST(Mailer  AS VARCHAR(100)) [Mailer],
        CAST([Database Name] AS VARCHAR(100)) [Database Name], 
        CAST((Convert(VARCHAR(10),dShipDateShipped,101) +' '+ CONVERT(VARCHAR(10),CAST(dShipDateShipped as time),100)) AS VARCHAR(100)) [ShipDate Shipped],
        CAST(cListName AS VARCHAR(100)) [cListName], 
        CAST([Decoy Key] AS VARCHAR(100)) [Decoy Key]
        FROM ##AmeriMarkResult
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        orders_df.to_csv(report_path, index=False)


def clean_up_sql():
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS ##AmeriMarkResult;
        """
        sql_cursor.execute(sql_script)
