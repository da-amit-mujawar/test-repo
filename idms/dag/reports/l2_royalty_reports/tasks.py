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


sql_output_bucket_name = 'idms-2722-internalfiles'
sql_output_folder = 'Reports/RoyaltyUsage/'


def generate_report(mode, report_name, report_path):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    sql_output_key = sql_output_folder + report_name

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DECLARE @lcStartDate  DATE, 
		@lcEndDate  DATE

        -- Daily, Weekly or Monthly
        IF '{mode}' = 'W'
            BEGIN
                SET @lcStartDate =  GETDATE() - 7  
                SET @lcEndDate   =  GETDATE() - 1  
            END
        ELSE
            BEGIN
                IF '{mode}' = 'D'
                    BEGIN
                        SET @lcStartDate = GETDATE() - 1
                        SET @lcEndDate   = GETDATE() - 0
                    END
                ELSE
                --Default Monthly
                    BEGIN
                        SET @lcStartDate  = DATEADD(month, DATEDIFF(month, 0, EOMONTH(GETDATE(), -1)), 0)
                        SET @lcEndDate    = EOMONTH(GETDATE(), -1) 
                    END
            END

        --CREATE Table in Tempdata
        DROP TABLE IF EXISTS ##RoyaltyL2_ToBeDropped
        CREATE TABLE ##RoyaltyL2_ToBeDropped(
            DatabaseID int NULL,
            OrderID int NULL,
            PO varchar(25) NULL,
            MailerPO varchar(30) NULL,
            ListName varchar(50) NULL,
            ShipDate varchar(30) NULL,
            Fieldname varchar(30) NULL,
            MailerID int NULL,
            MailerName varchar(30) NULL,
            OR_InSelection varchar(3) NULL,
            OrderLevelFilter varchar(3) NULL,
            OrderQty int NULL,	
            SegmentQty int NULL,
            OutputQty int NULL,
            AccountingDivision varchar(30) NULL,
            UnitCost numeric(4, 4) NOT NULL,
            TotalCost varchar(30) NULL,
            ReShipped varchar(3) NOT NULL,
            NoUsageChecked varchar(3) NULL
            )    

            INSERT INTO	##RoyaltyL2_ToBeDropped
            SELECT	DB.ID as DatabaseID,
                        OS.OrderID as OrderID,
                        LEFT(O.cLVAOrderNo, 25) PO,
                        LEFT(O.cBrokerPONo, 30) MailerPO,
                        LEFT(DB.cDatabaseName, 50) ListName,
                        CAST(OS.dCreatedDate AS varchar(30)) ShipDate,
                        CAST(SS.cQuestionFieldName AS varchar(30)) Fieldname,
                        CASE WHEN(O.DivisionMailerID <> 0)   THEN O.DivisionMailerID  ELSE O.MailerID  END AS MailerID, 
                        CAST((CASE WHEN(DM.ID IS NOT NULL) THEN DM.cCompany ELSE M.cCompany END) AS varchar(30)) AS MailerName, 
                        CAST((CASE WHEN(SS.cJoinOperator = 'OR') THEN 'YES' ELSE 'no' END) AS char(3)) AS OR_InSelection,
                        CAST((CASE WHEN(S.iIsOrderLevel = 1) THEN 'YES' ELSE 'no' END) AS char(3)) AS OrderLevelFilter,
                        O.iProvidedCount OrderQty,
                        S.iProvidedQty SegmentQty,
                        CASE WHEN S.iOutputQty < 0 THEN S.iProvidedQty ELSE S.iOutputQty END OutputQty ,
                        LEFT(LK.cDescription, 30) AccountingDivision,
                        0.0020 as UnitCost,
                        CAST(CAST(ROUND(CASE WHEN S.iOutputQty < 0 THEN S.iProvidedQty WHEN S.iOutputQty =0 Then 0 ELSE S.iOutputQty END * .002,2) as DECIMAL(16,2)) AS VARCHAR(30)) as TotalCost,
                        'no' as ReShipped,
                        CASE WHEN(iIsNoUsage = 1) THEN 'YES' ELSE 'no' END NoUsageChecked
            FROM	dw_Admin.dbo.tblOrder AS O with (nolock)
                        INNER JOIN dw_Admin.dbo.tblBuild AS B with (nolock)				ON B.ID = O.BuildID 
                        INNER JOIN dw_Admin.dbo.tblDatabase AS DB with (nolock)			ON DB.ID = B.DatabaseID 
                        INNER JOIN DW_Admin.dbo.tblLookup LK with (nolock)				ON DB.LK_AccountingDivisionCode = LK.cCode AND LK.cLookupValue = 'AccountingDivisionCode'
                        INNER JOIN dw_Admin.dbo.tblDivision AS DV with (nolock)			ON DV.ID = DB.DivisionID 
                        INNER JOIN dw_Admin.dbo.tblOrderStatus AS OS with (nolock)		ON OS.OrderID = O.ID AND OS.iStatus = 130 AND OS.iIsCurrent = 1 
                        INNER JOIN dw_Admin.dbo.tblSegment AS S with (nolock)			ON S.OrderID = o.ID 
                        INNER JOIN dw_Admin.dbo.tblSegmentSelection AS SS with (nolock)	ON SS.SegmentID = S.ID 
                        LEFT JOIN dw_Admin.dbo.tblMailer AS M with (nolock)				ON M.ID = O.MailerID 
                        LEFT JOIN dw_Admin.dbo.tblDivisionMailer AS DM with (nolock)	ON DM.ID = O.DivisionMailerID 
            WHERE		(OS.dCreatedDate BETWEEN @lcStartDate AND @lcEndDate) 
                        --AND DB.ID in (82)	all databases
                        --AND S.iProvidedQty > 0	user wants order level to appear
                        --AND iIsNoUsage = 0
                        AND Left(UPPER(LEFT(SS.cQuestionFieldName,4)), 3) = 'L2_' 
                        --AND UPPER(LEFT(SS.cQuestionFieldName,4)) like 'L2_%' 
            GROUP BY	DB.ID, OS.OrderID, O.cLVAOrderNo, O.cBrokerPONo, DB.cDatabaseName, OS.dCreatedDate, SS.cQuestionFieldName , 
                        O.DivisionMailerID, O.MailerID, DM.ID, DM.cCompany, M.cCompany, 
                        SS.cJoinOperator, S.iIsOrderLevel, O.iProvidedCount, S.iProvidedQty, S.iOutputQty, LK.cDescription, iIsNoUsage
            ORDER BY 1,2

            --FLAG RESHIPPED ORDERS
                DROP TABLE IF EXISTS #Temp_ReshippedOrders
                    SELECT OrderID
                    INTO #Temp_ReshippedOrders
                    FROM DW_Admin.dbo.tblOrderStatus WITH (NOLOCK)
                    WHERE iStatus = 130
                        AND OrderID  IN (SELECT DISTINCT ORDERID FROM ##RoyaltyL2_ToBeDropped)
                GROUP BY OrderID 
                    HAVING COUNT(*) > 1

                    UPDATE ##RoyaltyL2_ToBeDropped
                    SET Reshipped = 'YES'
                    FROM ##RoyaltyL2_ToBeDropped 
                    WHERE OrderID in (select OrderID from #Temp_ReshippedOrders)
         
        """
        print(sql_script)
        sql_cursor.execute(sql_script)
        sql_script = f"""
        	SELECT 
		DatabaseID
		,OrderID
		,PO
		,MailerPO
		,MailerName
		,ListName
		,ShipDate
		,FieldName
		,OR_InSelection
		,ReShipped
		,NoUsageChecked
		,OrderLevelFilter
		,OrderQty
		,SegmentQty
		,OutputQty
--		,UnitCost
--		,TotalCost
--		,AccountingDivision 
	    FROM ##RoyaltyL2_ToBeDropped 
	    ORDER BY 1, 2;
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        time_stamp = datetime.today().strftime('%Y%m%d')
        s3_key = sql_output_key.replace('{yyyymmdd}', time_stamp)
        save_dataframe(sql_output_bucket_name, s3_key, orders_df, '|')
        orders_df.to_csv(report_path, '|', index=False,)
