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
from helpers.s3 import save_dataframe, delete_file

database_id = 1150

sql_output_bucket_name = 'idms-2722-internalfiles'
sql_output_key = 'internaltransfer/Temp/RoyaltyOrderIds_1150.csv'

royalty_fields_bucket_name = 'idms-2722-internalfiles'
royalty_fields_output_key = 'Reports/RoyaltyUsage/RoyaltyFieldNames_1150.txt'

fields = 'Bus_ExecutiveSourceCode|Bus_VendorID|Cons_VendorID'


def export_fields():
    fields_dict = {'cFieldName': fields.split('|')}
    fields_df = pd.DataFrame(fields_dict)
    save_dataframe(royalty_fields_bucket_name,
                   royalty_fields_output_key, fields_df, '|')


def export_royalty_orderids_from_sql():
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    month_range = 1  # VC JOB VARIABLE. Data is available for month_range = 4.

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        -- Task 2 started
        DECLARE @MONTHRANGE TINYINT = {month_range}, @TEMPDATE DATETIME,
        @BeginDate VARCHAR(20),
        @EndDate VARCHAR(20)

        SET @TEMPDATE = DATEADD(MONTH, @MONTHRANGE*-1, GetDate())
        SET @BeginDate =LEFT(CONVERT(VARCHAR(100), @TEMPDATE, 121), 8) +'01'
        SET @EndDate = LEFT(CONVERT(VARCHAR(100), GETDATE(), 121), 8)+'01'

        SELECT @BeginDate
        SELECT @EndDate

        DROP TABLE IF EXISTS #RoyaltyOrderIdTable;


        CREATE TABLE #RoyaltyOrderIdTable
        (
            OrderId varchar(10) NULL,
            cTableName varchar(50) NOT NULL,
            CreatedBy varchar(50) NULL,
            ShippedBy varchar(50) NULL,
            Mailer varchar(100) NULL, 
            cNextMarkOrderNo varchar(50) NULL,
            PO varchar(50) NULL,
            OrderDescription varchar(50) NULL,
            ListName varchar(50) NULL,
            OrderQty varchar(12) NULL,
            ShipDate varchar(10) NULL,
            cNotes varchar(50) NULL,
            Bus_ContactName_Exported varchar(3) NULL, 
            Bus_EmailAddress_Exported varchar(3) NULL, 
            Cons_EmailAddress_Exported varchar(3) NULL 
        );

        INSERT INTO #RoyaltyOrderIdTable (CreatedBy, ShippedBy, OrderId, Mailer, cNextMarkOrderNo, PO, OrderDescription, ListName, OrderQTY, ShipDate, cTableName, cNotes) 
        SELECT '' AS CreatedBy,
        U.cFirstName + ' ' + U.cLastName AS ShippedBy,
        O.Id AS Orderid,
        CASE
            WHEN (Divm.ID IS NOT NULL)
            THEN Divm.cCompany
            ELSE ML.cCompany
        END AS Mailer,
        O.cNextMarkOrderNo, 
        O.cLVaOrderNo AS PO,
        O.cDescription AS OrderDescription,
        Dat.cDatabaseName AS ListName,
        O.iProvidedCount AS OrderQTY,
        CONVERT(VARCHAR(10), OS.dCreatedDate, 101) AS ShipDate,
        BT.cTableName AS cTableName,
        Left(O.cNotes, 50) AS cNotes
        FROM DW_Admin.dbo.tblorder O
        JOIN DW_Admin.dbo.tblBuild B ON O.BuildId=B.id
        JOIN DW_Admin.dbo.tblDatabase Dat ON B.DatabaseID=Dat.Id
        JOIN DW_Admin.dbo.tblDivision Div ON Dat.DivisionID=Div.Id /*Added NEW LEFT join for tblDivisionMailer,tblMailer */
        LEFT OUTER JOIN dw_admin.dbo.tblDivisionMailer Divm ON O.DivisionMailerID = Divm.ID
        AND O.DivisionMailerID <>0 /*Added NEW LEFT join for tblDivisionMailer,tblMailer */
        LEFT OUTER JOIN dw_admin.dbo.tblMailer ML ON Dat.Id =ML.DatabaseID
        JOIN DW_Admin.dbo.tblOrderStatus OS ON O.id=OS.OrderID
        JOIN DW_Admin.dbo.tbluser U ON OS.cCreatedBy=U.cUserID
        JOIN DW_Admin.dbo.tblBuildTable BT ON B.id=BT.BuildID
        WHERE Dat.Id={database_id}
        AND OS.iStatus=130
        AND OS.iIsCurrent=1
        AND (OS.dCreatedDate > @BeginDate
            AND OS.dCreatedDate < @EndDate)
        AND BT.LK_TableType ='M'
        ORDER BY CAST(O.Id AS varchar(10)),
                BT.cTableName;


        CREATE INDEX IX_RoyaltyOrderIdTable_Orderid ON #RoyaltyOrderIdTable (Orderid);


        DROP TABLE IF EXISTS #OrderStatusIDs
        CREATE TABLE #OrderStatusIDs (OrderId varchar(10) NULL,ID int);

        INSERT INTO #OrderStatusIDs(OrderId, ID)
        SELECT A.OrderID,Max(ID) AS ID
        FROM DW_Admin.dbo.tblOrderStatus A
        INNER JOIN #RoyaltyOrderIdTable B ON A.OrderID = B.OrderID
        AND iStatus=10
        AND iIsCurrent=0
        GROUP BY A.OrderID;

        /*  now update CreatedBy  */
        UPDATE #RoyaltyOrderIdTable
        SET CreatedBy = U.cFirstName + ' ' + U.cLastName --Select A.Orderid,U.*

        FROM #RoyaltyOrderIdTable A
        INNER JOIN DW_Admin.dbo.tblOrderStatus OS ON A.Orderid=OS.OrderID
        INNER JOIN DW_Admin.dbo.tbluser U ON OS.cCreatedBy=U.cUserID
        INNER JOIN #OrderStatusIDs OrderStatusIDs ON OrderStatusIDs.ID = OS.ID
        AND OS.iStatus=10
        AND OS.iIsCurrent=0;

        /*cleanups*/
        UPDATE #RoyaltyOrderIdTable
        SET cNotes = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cNotes, '''', ''), '"', ''), '|', ''), char(10), ''), char(13), ''),
            Bus_ContactName_Exported ='No',
            Bus_EmailAddress_Exported ='No',
            Cons_EmailAddress_Exported ='No' ;

        /*Bus_ContactName_Exported*/
        UPDATE #RoyaltyOrderIdTable
        SET Bus_ContactName_Exported ='Yes'
        FROM #RoyaltyOrderIdTable A
        INNER JOIN DW_Admin.dbo.tblOrderExportLayout B ON A.Orderid =B.OrderID
        WHERE (cFieldname ='Bus_ContactName'
            OR cCalculation like '%Bus_ContactName%');

        /*Bus_EmailAddress_Exported*/
        UPDATE #RoyaltyOrderIdTable
        SET Bus_EmailAddress_Exported ='Yes'
        FROM #RoyaltyOrderIdTable A
        INNER JOIN dw_admin.dbo.tblOrderExportLayout B WITH (NOLOCK) ON A.Orderid =B.OrderID
        WHERE (cFieldname ='Bus_EmailAddress'
            OR cCalculation like '%Bus_EmailAddress%');

        /*Cons_EmailAddress_Exported*/
        UPDATE #RoyaltyOrderIdTable
        SET Cons_EmailAddress_Exported ='Yes'
        FROM #RoyaltyOrderIdTable A
        INNER JOIN dw_admin.dbo.tblOrderExportLayout B WITH (NOLOCK) ON A.Orderid =B.OrderID
        WHERE (cFieldname ='cons_emailaddress'
            OR cCalculation like '%cons_emailaddress%');

        /*Drop Orderids if all the flags shows  'No'*/
        DELETE
        FROM #RoyaltyOrderIdTable
        WHERE (Bus_ContactName_Exported ='No'
            AND Bus_EmailAddress_Exported='No'
            AND Cons_EmailAddress_Exported ='No');

        /*End of Additinal Logic */
        DROP TABLE IF EXISTS #RoyaltyTemp_{database_id}_ToBeDropped;


        SELECT * INTO #RoyaltyTemp_{database_id}_ToBeDropped
        FROM #RoyaltyOrderIdTable; 

         -- Task 3 Started
        DROP Table IF EXISTS ##RoyaltyOrderIds_{database_id}_ToBeDropped;
        
        SELECT Orderid, 
        cTableName, 
        CreatedBy, 
        ShippedBy, 
        Mailer ,         
        cNextMarkOrderNo , 
        PO, 
        OrderDescription, 
        ListName, 
        OrderQty, 
        ShipDate, 
        cNotes, 
        Bus_ContactName_Exported,  
        Bus_EmailAddress_Exported,  
        Cons_EmailAddress_Exported
        INTO ##RoyaltyOrderIds_{database_id}_ToBeDropped
        FROM #RoyaltyTemp_{database_id}_ToBeDropped; 
        """
        sql_cursor.execute(sql_script)
        sql_script = f"""
        Select top 100 * from  ##RoyaltyOrderIds_{database_id}_ToBeDropped;
        """
        orders_df = sqlhook.get_pandas_df(sql_script)
        save_dataframe(sql_output_bucket_name, sql_output_key, orders_df, '|')


def get_royalty_records_redshift():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS  RoyaltyOrderWithFields_{database_id}_ToBeDropped;

        CREATE TABLE RoyaltyOrderWithFields_{database_id}_ToBeDropped
        (OrderId varchar(10),
        cTableName varchar(50),
        cFieldName varchar(50),
        processed varchar(1)
        );
        
        INSERT INTO RoyaltyOrderWithFields_{database_id}_ToBeDropped(OrderID, cTableName, cFieldName, processed)
        SELECT DISTINCT A.Orderid,
                        A.cTableName,
                        b.cFieldName,
                        '' AS processed
        FROM RoyaltyOrderIds_{database_id}_ToBeDropped A,RoyaltyFields_{database_id}_ToBeDropped B;

        DROP TABLE IF EXISTS RoyaltyOutput_{database_id}_ToBeDropped;

        CREATE TABLE RoyaltyOutput_{database_id}_ToBeDropped (
            OrderId varchar(10),
            Fieldname varchar(50),
            Fieldvalues varchar(50),
            Counts int
        );
        """
        redshift_cursor.execute(sql_script)
        sql_count = f"""
        SELECT Count(*) FROM RoyaltyOrderWithFields_{database_id}_ToBeDropped WHERE processed='' 
        """
        result = redshift_hook.get_first(sql_count)
        total_records = int(result[0])
        position = 1
        while position <= total_records:
            sql_script = f"""            
            SELECT TOP 1 
            Orderid,
            ctablename,
            cFieldname
            FROM RoyaltyOrderWithFields_{database_id}_ToBeDropped
            WHERE processed=''
            ORDER BY Orderid,cTableName,cFieldName
            """
            redshift_cursor.execute(sql_script)
            row = redshift_hook.get_first(sql_script)
            order_id, table_name, field_name = row[0], row[1], row[2]
            field_name_script = f"select top 1 * from information_schema.columns A WHERE Upper(A.table_name)= Upper('{table_name}') and Upper(A.column_name)= Upper('{field_name}')"
            count_table_script = f"select top 1 * from information_schema.columns where Upper(table_name) = Upper('{table_name}')"
            build_script = f"select top 1 * from information_schema.columns where Upper(table_name)= Upper('Count_{order_id}')"
            sql_script = f"""
            SELECT EXISTS({count_table_script}) 
               AND Exists({count_table_script})
               AND EXISTS({field_name_script});
               """
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                sql_script = f"""
                SELECT EXISTS(Select * from FROM RoyaltyOrderWithFields_{database_id}_ToBeDropped WHERE processed ='');
                """
                result = redshift_hook.get_first(sql_script)
                if result[0]:
                    insert_script = f"""
                    INSERT INTO RoyaltyOutput_{database_id}_ToBeDropped
                    SELECT '{order_id}' OrderId,
                        '{field_name}' FieldName,
                        B.{field_name} Fieldvalues,
                        Count(*) Counts
                    FROM Count_{order_id} A
                    INNER JOIN {table_name} B ON A.dwid =B.id
                    GROUP BY B.{field_name}            
                    """
                    redshift_hook.run(insert_script)
            else:
                error_log = ''
                sql_script = f"""
                SELECT NOT EXISTS({field_name_script});
                """
                result = redshift_hook.get_first(sql_script)
                if result[0]:
                    error_log = field_name + ' is not in Maintable'
                sql_script = f"""
                SELECT NOT EXISTS({count_table_script});
                """
                result = redshift_hook.get_first(sql_script)
                if result[0]:
                    error_log = 'Count table not found. Probably archived'
                sql_script = f"""
                SELECT NOT EXISTS({build_script});
                """
                result = redshift_hook.get_first(sql_script)
                if result[0]:
                    error_log = 'Build no longer on disk'
                insert_script = f"""
                INSERT INTO RoyaltyOutput_{database_id}_ToBeDropped Values ('{order_id}','field_name','{error_log}',0)
                """
                redshift_hook.run(insert_script)
            update_script = f"""
            UPDATE RoyaltyOrderWithFields_{database_id}_ToBeDropped
            SET processed='Y'
            WHERE orderid='{order_id}'
            AND ctablename='{table_name}'
            AND cFieldname= '{field_name}'
            AND processed=''
            """
            redshift_hook.run(update_script)
            position = position + 1
        sql_script = f"""
        DROP TABLE IF EXISTS RoyaltyOutput_ForExport_{database_id}_ToBeDropped;
        SELECT * INTO RoyaltyOutput_ForExport_{database_id}_ToBeDropped FROM (
        SELECT CONCAT('"',CONCAT('CreatedBy','"')) CreatedBy,
        CONCAT('"',CONCAT('ShippedBy','"')) ShippedBy,
        CONCAT('"',CONCAT('Orderid','"')) Orderid,
        CONCAT('"',CONCAT('Mailer','"')) Mailer,
        CONCAT('"',CONCAT('cNextMarkOrderNo','"')) cNextMarkOrderNo,
        CONCAT('"',CONCAT('PO','"')) PO,
        CONCAT('"',CONCAT('OrderDescription','"')) OrderDescription,
        CONCAT('"',CONCAT('Listname','"')) Listname,
        CONCAT('"',CONCAT('OrderQty','"')) OrderQty,
        CONCAT('"',CONCAT('ShipDate','"')) ShipDate,
        CONCAT('"',CONCAT('FieldName','"')) FieldName,
        CONCAT('"',CONCAT('Vendor ID','"')) "Vendor ID",
        CONCAT('"',CONCAT('Qty Shipped','"')) "Qty Shipped",
        CONCAT('"',CONCAT('Bus_ContactName_Exported','"')) Bus_ContactName_Exported,
        CONCAT('"',CONCAT('Bus_EmailAddress_Exported','"')) Bus_EmailAddress_Exported,
        CONCAT('"',CONCAT('Cons_EmailAddress_Exported','"')) Cons_EmailAddress_Exported,
        CONCAT('"',CONCAT('Notes','"')) Notes,
        1 as iOrderBy
        UNION
        SELECT DISTINCT CONCAT('"',CONCAT(A.CreatedBy,'"')) CreatedBy,
        CONCAT('"',CONCAT(A.ShippedBy,'"')) ShippedBy,
        CONCAT('"',CONCAT(A.Orderid,'"')) Orderid,
        CONCAT('"',CONCAT(A.Mailer,'"')) Mailer,
        CONCAT('"',CONCAT(A.cNextMarkOrderNo,'"')) cNextMarkOrderNo,
        CONCAT('"',CONCAT(A.PO,'"')) PO,
        CONCAT('"',CONCAT(A.OrderDescription,'"')) OrderDescription,
        CONCAT('"',CONCAT(A.Listname,'"')) Listname,
        CONCAT('"',CONCAT(A.OrderQty,'"')) OrderQty,
        CONCAT('"',CONCAT(A.ShipDate,'"')) ShipDate,
        CONCAT('"',CONCAT(B.FieldName,'"')) FieldName,
        CONCAT('"',CONCAT(B.FieldValues,'"')) "Vendor ID",
        CONCAT('"',CONCAT(CAST(B.Counts AS VarChar),'"')) "Qty Shipped",
        CONCAT('"',CONCAT(A.Bus_ContactName_Exported,'"')) Bus_ContactName_Exported,
        CONCAT('"',CONCAT(A.Bus_EmailAddress_Exported,'"')) Bus_EmailAddress_Exported,
        CONCAT('"',CONCAT(A.Cons_EmailAddress_Exported,'"')) Cons_EmailAddress_Exported,
        CONCAT('"',CONCAT(A.cNotes,'"')) Notes,
        2 as iOrderBy
        FROM RoyaltyOrderIds_{database_id}_ToBeDropped A
        INNER JOIN RoyaltyOutput_{database_id}_ToBeDropped B ON A.Orderid= b.Orderid
         ) as tmp;
        """
        redshift_cursor.execute(sql_script)


def clean_up_sql():
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS Tempdata.dbo.RoyaltyTemp_{database_id}_ToBeDropped;
        DROP VIEW IF EXISTS  View_RoyaltyOrderIds_{database_id}_ToBeDropped;
        """
        sql_cursor.execute(sql_script)


def delete_files():
    delete_file(sql_output_bucket_name, sql_output_key)
