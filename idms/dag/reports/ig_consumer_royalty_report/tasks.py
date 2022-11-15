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


# sql_output_bucket_name = 'idms-2722-internalfiles'
sql_output_bucket_name = Variable.get('var-manifest-bucket')
sql_output_folder_new = 'Reports/RoyaltyUsage/'


def get_royalty_orderids_from_sq(mode, database_id,sql_output_folder, **kwargs):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    # to trigger dag w/dates:
    # {"run-info": {"mode": "DR", "start-date": "2022.02.27", "end-date": "2022.03.05"} }
    start_date = ''
    end_date = ''
    if len(kwargs['dag_run'].conf) >= 1:
        mode = 'DR'
        start_date = kwargs['dag_run'].conf.get('run-info')['start-date']
        end_date = kwargs['dag_run'].conf.get('run-info')['end-date']
    print('Job running mode is : ', mode)
    print('Start Date is : ', start_date)
    print('End Date is : ', end_date)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        /* 
        Database: 1267
        Field Name: Vendor_Code
        Frequency: Weekly
        Distribution List: DataAcqGroup@infogroup.com, Cory.Pawloski@infogroup.com, DL-IDMSAdminInfogroup@infogroup.com
        One thing we need to keep in mind, is that Vendor_Code will not be populated 100%. We should include a row in the report for blank vendor codes, so Cory knows how many records on the order were Infogroup records.
        
        Same Columns as 992’s report
        
        ShippedBy
        Orderid
        Mailer
        cNextMarkOrderNo
        PO
        OrderDescription
        Listname
        OrderQty
        ShipDate
        FieldName
        FieldValue
        Qty Shipped
        Contact Vendor
        Email Vendor
        OESSOrderID
        OESSAccountNumber
        OESSInvoiceTotal
        SalesRepName
        DivisionNumber
        
        SF 10.25.2019
        */

        
        DECLARE @lcSQL varchar (8000) =''
        DECLARE @inString varchar (8000) =''
        DECLARE @likeString varchar (8000) =''
        DECLARE @fieldList varchar(800) =''
        DECLARE @databaseID int
        DECLARE	@BeginDate smalldatetime, @EndDate smalldatetime 


        -- Daily, Weekly or Monthly
        IF '{mode}' = 'W'
            BEGIN
                SET @BeginDate = GETDATE() - 7
                SET @EndDate   = GETDATE() - 1
                
                
            END
        ELSE
            BEGIN
                IF '{mode}' = 'DR'
                    BEGIN
                        SET @BeginDate = '{start_date}'
                        SET @EndDate = '{end_date}'
                    END
                ELSE
                    BEGIN
                        IF '{mode}' = 'D'
                            BEGIN
                                SET @BeginDate = GETDATE() - 1
                                SET @EndDate   = GETDATE() - 0
                            END
                        ELSE
                            --Default Monthly
                             BEGIN
                                SET @BeginDate  = DATEADD(month, DATEDIFF(month, 0, EOMONTH(GETDATE(), -1)), 0)
                                SET @EndDate    = EOMONTH(GETDATE(), -1) 
                             END
                    END
            END

        --Weekly 
        --SET @BeginDate=DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -8)
        /*
        SET @BeginDate=DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -6)
        SET @EndDate =DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0.9999999)
        */



        --For one time report only remove!!!!
        --SET @BeginDate ='2020-10-18' --DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), -7)
        --SET @EndDate   ='2020-10-24'

        --Get the list of all orders for the time frame and database
        DROP TABLE IF EXISTS #OrderIds;
        CREATE TABLE #OrderIds  (Orderid int  , ContactFlag Varchar(1) NULL,  EmailFlag Varchar(1)NULL, Shipdate varchar(10) NULL   );

        INSERT INTO #OrderIds (Orderid, ContactFlag, EmailFlag,Shipdate  )
        SELECT DISTINCT O.ID, 'N', 'N',   CONVERT(VARCHAR(10),OS.dCreatedDate, 102) as Shipdate
            FROM DW_ADMIN.DBO.tblOrder O with (nolock) 
            JOIN DW_Admin.dbo.tblBuild B with (nolock)  ON O.BuildId=B.id
            JOIN DW_Admin.dbo.tblDatabase Dat with (nolock)  on B.DatabaseID=Dat.Id
            JOIN DW_Admin.dbo.tblOrderStatus OS with (nolock)  on OS.OrderID= O.Id 
        WHERE OS.iStatus=130  AND OS.iIsCurrent=1  
        --AND (OS.dCreatedDate >=@BeginDate AND OS.dCreatedDate <= @EndDate  )   
        AND (OS.dCreatedDate BETWEEN @BeginDate AND @EndDate )   
        --AND (OS.dCreatedDate BETWEEN '2021.05.10' AND '2021.10.21' )
        AND Dat.Id={database_id}  
        ORDER BY  O.ID

        /*
        Logic:
        check Order filter  for the fields Check Export ExportLayout for the fields
        we will check Vendor_ID   <>36 in IQ.
        */   

        --update emailflag to yes if any of the royalty fields included in selection on the segment
        UPDATE #OrderIds SET EmailFlag ='Y' FROM #OrderIds AS A JOIN dw_admin.dbo.tblsegment SL with (nolock) 
        ON SL.OrderID=A.Orderid JOIN dw_admin.dbo.tblsegmentselection SS with (nolock) 
        ON SL.ID=SS.SegmentID  Where EmailFlag <> 'Y' AND SS.cQuestionFieldName='EmailAddress' 

        --update emailflag to yes if any of the fields in the list is in the layout or used for calculation
        UPDATE #OrderIds SET EmailFlag = 'Y' FROM #OrderIds AS A JOIN dw_admin.dbo.tblOrderExportLayout ELD with (nolock)
        ON ELD.OrderID=A.Orderid Where  EmailFlag <> 'Y' AND (ELD.cFieldName='EmailAddress' OR  ELD.cCalculation like '%EmailAddress%')


        DROP TABLE IF EXISTS #OrderIdWithExportFields;
        CREATE TABLE #OrderIdWithExportFields
        (
            OrderId varchar(10) NULL,
            cTableName varchar(50) NOT NULL,
            ContactFlag Varchar(1) NULL,
            EmailFlag Varchar(1) NULL,
        --	CreatedBy varchar(50) NULL,
            ShippedBy varchar(50) NULL,
            Mailer varchar(100) NULL, 
            cNextMarkOrderNo varchar(50) NULL,
            PO varchar(50) NULL,
            OrderDescription varchar(50) NULL,
            ListName varchar(50) NULL,
            OrderQty varchar(12) NULL,
            ShipDate varchar(10) NULL,
            cNotes varchar(100) NULL, 
            OESSOrderID varchar(30) NULL,
            OESSAccountNumber varchar(30) NULL,
            OESSInvoiceTotal varchar(12) NULL,
            SalesRepName varchar (50) NULL,
            DivisionNumber varchar (50) NULL
        ); 

        INSERT INTO  #OrderIdWithExportFields
        (
            OrderId,
            cTableName,
            ContactFlag,
            EmailFlag, 
        --	CreatedBy,
            ShippedBy,
            Mailer,
            cNextMarkOrderNo, 
            PO,
            OrderDescription, 
            ListName,	
            OrderQTY,
            ShipDate,
        --	cNotes, 
            OESSOrderID,
            OESSAccountNumber,
            OESSInvoiceTotal,
            SalesRepName,
            DivisionNumber,
            cNotes
            )
            SELECT distinct
                O.Id  as  Orderid,
                BT.cTableName as  cTableName , 
                tempOrderid.ContactFlag,
                tempOrderid.EmailFlag,
            --    '' As CreatedBy,
                U.cFirstName + ' ' + U.cLastName AS ShippedBy,
                CASE WHEN (Divm.ID is NOT NULL) THEN Divm.cCompany ELSE ML.cCompany END as Mailer,  
                O.cNextMarkOrderNo, 
                O.cLVaOrderNo as PO,
                O.cDescription as OrderDescription,
                Dat.cDatabaseName as ListName,
                O.iProvidedCount as OrderQTY,
                CONVERT(VARCHAR(12),OS.dCreatedDate,101) as ShipDate,
                OB.OESSOrderID as OESSOrderID,
                OB.cOESSAccountNumber as OESSAccountNumber,
                CAST(OB.iOESSInvoiceTotal AS VARCHAR(12)) as OESSInvoiceTotal,
                S.cSalesRepName as SalesRepName,
                S.cDivisionNo as DivisionNumber,
                Left(O.cNotes,100) as cNotes
            from #OrderIds tempOrderid 
                inner join DW_Admin.dbo.tblOrder O On tempOrderid.Orderid = O.id
                JOIN DW_Admin.dbo.tblBuild B  ON O.BuildId=B.id
                JOIN DW_Admin.dbo.tblDatabase Dat  on B.DatabaseID=Dat.Id
                JOIN DW_Admin.dbo.tblDivision Div  ON Dat.DivisionID=Div.Id
                LEFT OUTER JOIN dw_admin.dbo.tblDivisionMailer Divm ON O.DivisionMailerID  = Divm.ID AND O.DivisionMailerID <> 0
                LEFT OUTER JOIN dw_admin.dbo.tblMailer ML  ON Dat.Id =ML.DatabaseID
                JOIN DW_Admin.dbo.tblOrderStatus OS  on O.id=OS.OrderID
                JOIN DW_Admin.dbo.tbluser U  on OS.cCreatedBy=U.cUserID
                JOIN DW_Admin.dbo.tblBuildTable BT  ON B.id=BT.BuildID
                LEFT OUTER JOIN DW_Admin.dbo.tblOrderBilling OB  ON O.id=OB.OrderID					--newly added fields
                LEFT OUTER JOIN DW_Admin.dbo.tblSalesRep S on OB.cSalesRepID=S.cSalesRepID	--newly addred fields	
            WHERE  OS.iStatus=130  AND OS.iIsCurrent=1 --(EmailFlag='Y' OR ContactFlag='Y') AND
            and BT.cTableName like 'tblMain%'  
            
            /*
            UNION
            
                SELECT distinct
                O.Id  as  Orderid,
                BT.cTableName + '_ToBeDropped' as  cTableName , 
                tempOrderid.ContactFlag,
                tempOrderid.EmailFlag,
            --    '' As CreatedBy,
                U.cFirstName + ' ' + U.cLastName AS ShippedBy,
                CASE WHEN (Divm.ID is NOT NULL) THEN Divm.cCompany ELSE ML.cCompany END as Mailer,  
                O.cNextMarkOrderNo, 
                O.cLVaOrderNo as PO,
                O.cDescription as OrderDescription,
                Dat.cDatabaseName as ListName,
                O.iProvidedCount as OrderQTY,
                CONVERT(VARCHAR(12),OS.dCreatedDate,101) as ShipDate,
                OB.OESSOrderID as OESSOrderID,
                OB.cOESSAccountNumber as OESSAccountNumber,
                CAST(OB.iOESSInvoiceTotal AS VARCHAR(12)) as OESSInvoiceTotal,
                S.cSalesRepName as SalesRepName,
                S.cDivisionNo as DivisionNumber,
                Left(O.cNotes,100) as cNotes
            from #OrderIds tempOrderid 
                inner join DW_Admin.dbo.tblOrder O On tempOrderid.Orderid = O.id
                JOIN DW_Admin.dbo.tblBuild B  ON O.BuildId=B.id
                JOIN DW_Admin.dbo.tblDatabase Dat  on B.DatabaseID=Dat.Id
                JOIN DW_Admin.dbo.tblDivision Div  ON Dat.DivisionID=Div.Id
                LEFT OUTER JOIN dw_admin.dbo.tblDivisionMailer Divm ON O.DivisionMailerID  = Divm.ID AND O.DivisionMailerID <> 0
                LEFT OUTER JOIN dw_admin.dbo.tblMailer ML  ON Dat.Id =ML.DatabaseID
                JOIN DW_Admin.dbo.tblOrderStatus OS  on O.id=OS.OrderID
                JOIN DW_Admin.dbo.tbluser U  on OS.cCreatedBy=U.cUserID
                JOIN DW_Admin.dbo.tblBuildTable BT  ON B.id=BT.BuildID
                LEFT OUTER JOIN DW_Admin.dbo.tblOrderBilling OB  ON O.id=OB.OrderID					--newly added fields
                LEFT OUTER JOIN DW_Admin.dbo.tblSalesRep S on OB.cSalesRepID=S.cSalesRepID	--newly addred fields	
            WHERE  OS.iStatus=130  AND OS.iIsCurrent=1 --(EmailFlag='Y' OR ContactFlag='Y') AND
            and BT.cTableName like 'tblMain%' 
        */

        --Staging table for IQ export
        DROP TABLE IF EXISTS ##RoyaltyTemp_{database_id}_ToBeDropped;
        Select * 
        INTO ##RoyaltyTemp_{database_id}_ToBeDropped  
        from #OrderIdWithExportFields; 

         -- Task 3 Started
        DROP Table IF EXISTS ##RoyaltyOrderIds_{database_id}_ToBeDropped;
        
        SELECT Orderid,
        cTableName,
        ContactFlag, 
        EmailFlag,
        --CreatedBy, 
        ShippedBy,
        REPLACE (Mailer,'|','') AS Mailer,                   
        cNextMarkOrderNo,
        PO,
        REPLACE (OrderDescription,'|','') AS OrderDescription,
        ListName,
        OrderQty,
        ShipDate,
    --	LEFT(REPLACE(REPLACE(REPLACE(cNotes,'|',''),char(13),''),char(10),''),100) AS cNotes,
        CASE WHEN OESSOrderID IS NULL THEN '' ELSE OESSOrderID END AS OESSOrderID,
        CASE WHEN OESSAccountNumber IS NULL THEN '' ELSE OESSAccountNumber END AS OESSAccountNumber, 
        CASE WHEN OESSInvoiceTotal IS NULL THEN '' ELSE OESSInvoiceTotal END AS OESSInvoiceTotal,
        CASE WHEN SalesRepName IS NULL THEN '' ELSE SalesRepName END AS SalesRepName,
        CASE WHEN DivisionNumber IS NULL THEN '' ELSE DivisionNumber END AS DivisionNumber, 
        LEFT(REPLACE(REPLACE(REPLACE(cNotes,'|',''),char(13),''),char(10),''),100) AS cNotes
        INTO ##RoyaltyOrderIds_{database_id}_ToBeDropped
        FROM ##RoyaltyTemp_{database_id}_ToBeDropped; 
        """
        print(sql_script)
        sql_cursor.execute(sql_script)
        sql_script = f"""
        	Select * from  ##RoyaltyOrderIds_{database_id}_ToBeDropped;
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())        
        save_dataframe(sql_output_bucket_name, sql_output_folder, orders_df, '|')
        

def get_main_table_list_redshift():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)    

    with closing(redshift_conn.cursor()) as redshift_cursor:  
        sql_script = f"""
        select table_name from information_schema.tables where upper(table_name) like upper('%TBLMAIN%');
        """
        print(sql_script)
        
        orders_df = redshift_hook.get_pandas_df(sql_script)
        list_converted = orders_df['table_name'].values.tolist()
        
        sql_script = ""
        for element in list_converted:
            if  sql_script == "":
                sql_script = f"('{element}')"
            else:
                sql_script = f"{sql_script}, ('{element}')"        

        sql_script = f"""
        DROP TABLE IF EXISTS TablesList_temp;
        CREATE TABLE TablesList_temp
        (
            table_name varchar(250)
        );
        insert into TablesList_temp values {sql_script};
        """
        print(sql_script)
        redshift_cursor.execute(sql_script) 



def get_royalty_records_redshift(database_id, report_name, report_path):
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    sql_output_key = sql_output_folder_new + report_name

    with closing(redshift_conn.cursor()) as redshift_cursor:  
        sql_script = f"""
        --Create a table with Order and all the fields requested
        DROP TABLE IF EXISTS RoyaltyOrderWithFields_{database_id}_ToBeDropped; 
        CREATE TABLE RoyaltyOrderWithFields_{database_id}_ToBeDropped
        (OrderId varchar(10),
        EmailFlag varchar(1),
        cTableName varchar(50),
        cFieldName varchar(50),
        processed varchar(1)
        ); 

        INSERT INTO RoyaltyOrderWithFields_{database_id}_ToBeDropped(OrderID, EmailFlag, cTableName,cFieldName,processed)
        SELECT distinct A.Orderid, A.EmailFlag,  A.cTableName,b.cFieldName, '' as processed
        FROM RoyaltyOrderIds_{database_id}_ToBeDropped A , RoyaltyFields_{database_id}_ToBeDropped B;


        --SF 02.18.2020 If tblmain is no longer in IQ, replace it with _tobedropped so the previoud build 
        --can be used for count if still in IQ.        

        UPDATE RoyaltyOrderWithFields_1267_tobedropped 
        SET ctablename = RoyaltyOrderWithFields_1267_tobedropped.ctablename + '_CUSTOMDROP'
        from (
        select A.ctablename from RoyaltyOrderWithFields_1267_tobedropped A LEFT JOIN TablesList_temp B ON  upper(ctablename) = upper(table_name) WHERE B.table_name is null) C
        where RoyaltyOrderWithFields_1267_tobedropped.ctablename = C.ctablename;

        --Create a output table for counts
        DROP TABLE IF EXISTS RoyaltyOutput_{database_id}_ToBeDropped; 
        CREATE TABLE RoyaltyOutput_{database_id}_ToBeDropped
        --(OrderId varchar(10), Fieldname varchar(50), Fieldvalues varchar(50),  Counts int); 
        (OrderId varchar(10), OrderType varchar(20), Fieldname varchar(50), Fieldvalues varchar(50),  Counts int); 
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
            EmailFlag,
            ctablename,
            cFieldname
            FROM RoyaltyOrderWithFields_{database_id}_ToBeDropped
            WHERE processed=''
            ORDER BY Orderid,cTableName,cFieldName
            """
            redshift_cursor.execute(sql_script)
            row = redshift_hook.get_first(sql_script)
            order_id, email_flag, table_name, field_name = row[0], row[1], row[2], row[3]
            field_name_script = f"select top 1 * from information_schema.columns A WHERE Upper(A.table_name)= Upper('{table_name}') and Upper(A.column_name)= Upper('{field_name}')"
            count_table_script = f"select top 1 * from information_schema.columns where Upper(table_name) = Upper('{table_name}')"
            build_script = f"select top 1 * from information_schema.columns where Upper(table_name)= Upper('Count_{order_id}')"
            sql_script = f"""
            SELECT EXISTS({count_table_script}) 
               AND Exists({build_script})
               AND EXISTS({field_name_script});
               """
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                sql_script = f"""
                SELECT EXISTS(Select * FROM RoyaltyOrderWithFields_{database_id}_ToBeDropped WHERE processed ='');
                """
                result = redshift_hook.get_first(sql_script)

                if email_flag == 'Y':
                    if result[0]:
                        insert_script = f"""
                        INSERT INTO RoyaltyOutput_{database_id}_ToBeDropped
                        SELECT '{order_id}' OrderId,
                            'EMAIL' OrderType,
                            '{field_name}' FieldName,
                            CAST(B.{field_name} AS VARCHAR) Fieldvalues,
                            Count(*) Counts
                        FROM Count_{order_id} A
                        INNER JOIN {table_name} B ON A.dwid =B.id
                        GROUP BY B.{field_name}            
                        """
                        redshift_hook.run(insert_script)
                else:
                    if result[0]:
                        insert_script = f"""
                        INSERT INTO RoyaltyOutput_{database_id}_ToBeDropped
                        SELECT '{order_id}' OrderId,
                            'POSTAL' OrderType,
                            '{field_name}' FieldName,
                            CAST(B.{field_name} AS VARCHAR) Fieldvalues,
                            Count(*) Counts
                        FROM Count_{order_id} A
                        INNER JOIN {table_name} B ON A.dwid =B.id WHERE B.MATCHLEVEL='N'
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
                INSERT INTO RoyaltyOutput_{database_id}_ToBeDropped Values ('{order_id}','{field_name}','{error_log}',0)
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
        SELECT 'ShippedBy' ShippedBy,
        'Orderid' Orderid,
        'Mailer'Mailer,
        'cNextMarkOrderNo' cNextMarkOrderNo,
        'PO' PO,
        'OrderDescription' OrderDescription,
        'Listname' Listname,
        'OrderQty' OrderQty,
        'ShipDate' ShipDate,
        'FieldName' FieldName,
        'FieldValue' FieldValue,
        'Qty Shipped' "Qty Shipped",
        'Contact Vendor' "Contact Vendor",
        'Email Vendor' "Email Vendor",
        'OESSOrderID' OESSOrderID,
        'OESSAccountNumber' OESSAccountNumber,
        'OESSInvoiceTotal' OESSInvoiceTotal,
        'SalesRepName' SalesRepName,
        'DivisionNumber' DivisionNumber,
        'Notes' Notes,
        1 as iOrderBy
        UNION
        SELECT DISTINCT A.ShippedBy ,
        A.Orderid ,
        A.Mailer ,
        A.cNextMarkOrderNo ,
        A.PO ,
        A.OrderDescription ,
        A.Listname , 
        A.OrderQty ,
        A.ShipDate ,
        B.FieldName,
        B.FieldValues as FieldValue,
        (CAST(B.Counts AS VarChar)) "Qty Shipped",
        A.ContactFlag as "Contact Vendor",
        A.EmailFlag as "Email Vendor",
        A.OESSOrderID ,
        A.OESSAccountNumber ,
        A.OESSInvoiceTotal ,
        A.SalesRepName ,
        A.DivisionNumber ,
        A.cNotes ,
        2 as iOrderBy
        FROM RoyaltyOrderIds_{database_id}_ToBeDropped A
        INNER JOIN RoyaltyOutput_{database_id}_ToBeDropped B ON A.Orderid= b.Orderid
         ) as tmp;
        """
        redshift_cursor.execute(sql_script)
        sql_script = f"""
        SELECT  Report.ShippedBy,
        Report.Orderid,
        Report.Mailer,
        Report.cNextMarkOrderNo,
        Report.PO,
        Report.OrderDescription,
        Report.Listname,
        Report.OrderQty,
        Report.ShipDate,
        Report.FieldName,
        Report.FieldValue,
        Report."Qty Shipped",
        Report."Contact Vendor",
        Report."Email Vendor",
        Report.OESSOrderID,
        Report.OESSAccountNumber,
        Report.OESSInvoiceTotal,
        Report.SalesRepName,
        Report.DivisionNumber,
        Report.Notes
        FROM RoyaltyOutput_ForExport_{database_id}_ToBeDropped Report
        ORDER BY Report.iOrderBy ASC;
        """
        print(sql_script)
        orders_df = redshift_hook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        time_stamp = datetime.today().strftime('%Y%m%d')
        s3_key = sql_output_key.replace('{yyyymmdd}', time_stamp)
        save_dataframe(sql_output_bucket_name, s3_key, orders_df, ',')
        orders_df.to_csv(report_path, ',', index=False,header=False)


def clean_up_sql(database_id):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS ##RoyaltyTemp_{database_id}_ToBeDropped;
        DROP TABLE IF EXISTS ##RoyaltyOrderIds_{database_id}_ToBeDropped;
        """
        sql_cursor.execute(sql_script)

        
