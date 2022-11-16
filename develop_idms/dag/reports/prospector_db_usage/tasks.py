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


def generate_report(report_path):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    database_id = 1094

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        Declare @lcMonth Varchar(2)
        Declare @lcYear Varchar(4)
        Declare @lcYearMonPre varchar(10)
        Declare @liDatabaseID int

        Set @liDatabaseID = {database_id} 
        Set @lcYearMonPre = CAST(CAST(DATEADD(MONTH,-1,GETDATE()) as DATE) as varchar(10))
        Set @lcYear = LEFT(@lcYearMonPre,4)
        Set @lcMonth = SUBSTRING(@lcYearMonPre,6,2)
            

        SELECT 
            DISTINCT TOR.ID 
        INTO ##Temp_ShippedOrder
        FROM DW_Admin.dbo.tblOrder AS TOR
        INNER JOIN DW_Admin.dbo.tblOrderStatus AS TOS
        ON TOR.ID = TOS.OrderID
        AND TOS.iIsCurrent = 1
        AND TOS.iStatus = 130
        AND TOR.iIsNoUsage = 0
        AND TOS.dCreatedDate > '2009-11-30'
        AND MONTH(TOS.dCreatedDate) = @lcMonth
        AND YEAR(TOS.dCreatedDate) = @lcYear
        INNER JOIN DW_Admin.dbo.tblBuild AS TBL
        ON TOR.BuildID = TBL.ID
        WHERE TBL.DatabaseID = @liDatabaseID
        ORDER BY TOR.ID

        SELECT
        TOR.id AS OrderId,
        MailerID,
        (CASE WHEN (TDM.ID IS NOT NULL) THEN TDM.cCompany
            ELSE TML.cCompany END) AS MailerName,
        CONVERT(varchar(10), dShipDateShipped, 110) AS ShippedDate,
        cNextMarkOrderNo INTO ##Temp_MailerInfo
        FROM DW_Admin.dbo.tblOrder TOR
        LEFT JOIN DW_Admin.dbo.tblMailer TML
        ON TOR.MailerID = TML.ID
        AND DivisionMailerID = 0
        LEFT JOIN DW_Admin.dbo.tblDivisionMailer TDM
        ON TOR.DivisionMailerID = TDM.id
        AND DivisionMailerID <> 0
        WHERE TOR.id IN (SELECT ID FROM ##Temp_ShippedOrder)  

        """
        print(sql_script)
        sql_cursor.execute(sql_script)
        sql_script = f"""
         SELECT
        CAST(MI.OrderId AS varchar(100)) [Order ID],
        CAST(MI.cNextMarkOrderNo AS varchar(100)) [NextMark OrderNo],
        CAST(MI.MailerId AS varchar(100)) [Mailer ID],
        CAST(MI.MailerName AS varchar(100)) [Mailer Name],
        CAST((CONVERT(varchar(10), ROA.dCreatedDate, 101)) AS varchar(100)) [Ship Date],
        CAST(ROA.ListId AS varchar(100)) [List ID],
        CAST(List.cListName AS varchar(100)) [List Name],
        CAST(ROA.nTotalContribution AS varchar(100)) [Quantity],
        'N' [NoUsageFlag]
        FROM ##Temp_MailerInfo MI
        INNER JOIN tblRandomOrderAllocation ROA
        ON MI.OrderId = ROA.OrderID
        INNER JOIN tblMasterLoL List
        ON ROA.ListId = List.ID
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        orders_df.to_csv(report_path, ',', index=False)
