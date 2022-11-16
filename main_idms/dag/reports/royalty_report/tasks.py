from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.email import send_email_smtp
import pandas as pd
from pandas.io import sql as psql
from helpers.s3 import save_dataframe
import json
import logging
import os
import os.path
from datetime import datetime
from urllib.parse import urlparse

import boto3
from airflow.models import Variable
from airflow.operators.email import EmailOperator

# sql_output_bucket_name = 'develop_idms-2722-internalfiles'
sql_output_bucket_name = Variable.get('var-manifest-bucket')
sql_output_folder_new = 'Reports/RoyaltyUsage/'


def get_royalty_orderids_from_sq(database_id,
                                 mode,
                                 start_date,
                                 end_date,
                                 sql_output_folder,
                                 fetch_query):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    logging.info(mode, start_date, end_date, database_id)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = fetch_query
        print(sql_script)
        sql_cursor.execute(sql_script)
        sql_script = f"""
        	Select * from  ##RoyaltyOrderIds_@databaseID_ToBeDropped;
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        save_dataframe(sql_output_bucket_name, sql_output_folder, orders_df, '|')


def get_main_table_list_redshift(database_id):
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
            if sql_script == "":
                sql_script = f"('{element}')"
            else:
                sql_script = f"{sql_script}, ('{element}')"

        sql_script = f"""
        DROP TABLE IF EXISTS TablesList_temp_{database_id};
        CREATE TABLE TablesList_temp_{database_id}
        (
            table_name varchar(250)
        );
        insert into TablesList_temp_{database_id} values {sql_script};
        """
        print(sql_script)
        redshift_cursor.execute(sql_script)


def get_royalty_records_redshift(database_id, report_name, report_path):
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    logging.info('report_name is : ', report_name)
    logging.info('database is : ', database_id)
    sql_output_key = sql_output_folder_new + report_name

    with closing(redshift_conn.cursor()) as redshift_cursor:
        if database_id == 992:
            ins_script = f"""
            INSERT INTO RoyaltyFields_{database_id}_ToBeDropped
            VALUES('EXECUTIVESOURCECODE'), ('Vendor_ID'); """
        elif database_id == 1267:
            ins_script = f"""
            INSERT INTO RoyaltyFields_{database_id}_ToBeDropped
            VALUES('Vendor_CODE'); """
        elif database_id == 1150:
            ins_script = f"""
            INSERT INTO RoyaltyFields_{database_id}_ToBeDropped
            VALUES('Bus_ExecutiveSourceCode'), ('Bus_VendorID'), ('Cons_VendorID'); """
        else:
            ins_script = f"""
            INSERT INTO RoyaltyFields_{database_id}_ToBeDropped
            VALUES('EMAILSOURCE'); """
        redshift_cursor.execute(ins_script)

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

        INSERT INTO RoyaltyOrderWithFields_{database_id}_ToBeDropped(OrderID, EmailFlag, cTableName, cFieldName, processed)
        SELECT distinct A.Orderid, A.EmailFlag,  A.cTableName,b.cFieldName, '' as processed
        FROM RoyaltyOrderIds_{database_id}_ToBeDropped A , RoyaltyFields_{database_id}_ToBeDropped B;


        --SF 02.18.2020 If tblmain is no longer in IQ, replace it with _tobedropped so the previoud build 
        --can be used for count if still in IQ.        

        UPDATE RoyaltyOrderWithFields_{database_id}_tobedropped 
        SET ctablename = RoyaltyOrderWithFields_{database_id}_tobedropped.ctablename + '_CUSTOMDROP'
        from (
        select A.ctablename from RoyaltyOrderWithFields_{database_id}_tobedropped A LEFT JOIN TablesList_temp_{database_id} B ON  upper(ctablename) = upper(table_name) WHERE B.table_name is null) C
        where RoyaltyOrderWithFields_{database_id}_tobedropped.ctablename = C.ctablename;

        --Create a output table for counts
        DROP TABLE IF EXISTS RoyaltyOutput_{database_id}_ToBeDropped; 
        CREATE TABLE RoyaltyOutput_{database_id}_ToBeDropped 
        (OrderId varchar(10), OrderType varchar(50), Fieldname varchar(50), Fieldvalues varchar(50),  Counts int);
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
            ORDER BY Orderid,cTableName,cFieldName;
            """
            redshift_cursor.execute(sql_script)
            row = redshift_hook.get_first(sql_script)
            order_id, email_flag, table_name, field_name = row[0], row[1], row[2], row[3]
            field_name_script = f"select top 1 * from information_schema.columns A WHERE Upper(A.table_name)= Upper('{table_name}') and Upper(A.column_name)= Upper('{field_name}')"
            build_script = f"select top 1 * from information_schema.columns where Upper(table_name) = Upper('{table_name}')"
            count_table_script = f"select top 1 * from information_schema.columns where Upper(table_name)= Upper('Count_{order_id}')"
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
                if database_id == 1267 or database_id == 1106:
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
                            '' OrderType,
                            '{field_name}' FieldName,
                            CAST(B.{field_name} AS VARCHAR) Fieldvalues,
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
        if database_id == 1150:
            sql_script = f"""
                    DROP TABLE IF EXISTS RoyaltyOutput_ForExport_{database_id}_ToBeDropped;
                    SELECT * INTO RoyaltyOutput_ForExport_{database_id}_ToBeDropped FROM (
                    SELECT 'CreatedBy' CreatedBy,
                    'ShippedBy' ShippedBy,
                    'Orderid' Orderid,
                    'Mailer'Mailer,
                    'cNextMarkOrderNo' cNextMarkOrderNo,
                    'PO' PO,
                    'OrderDescription' OrderDescription,
                    'Listname' Listname,
                    'OrderQty' OrderQty,
                    'ShipDate' ShipDate,
                    'FieldName' FieldName,
                    'Vendor ID' "Vendor ID",
                    'Qty Shipped' "Qty Shipped",
                    'Bus_ContactName_Exported' "Bus_ContactName_Exported",
                    'Bus_EmailAddress_Exported' "Bus_EmailAddress_Exported",
                    'Cons_EmailAddress_Exported' "Cons_EmailAddress_Exported",
                    'Notes' Notes,
                    1 as iOrderBy
                    UNION
                    SELECT DISTINCT A.CreatedBy ,
                    A.ShippedBy ,
                    A.Orderid ,
                    A.Mailer ,
                    A.cNextMarkOrderNo ,
                    A.PO ,
                    A.OrderDescription ,
                    A.Listname , 
                    A.OrderQty ,
                    A.ShipDate ,
                    B.FieldName,
                    B.FieldValues as "Vendor ID",
                    (CAST(B.Counts AS VarChar)) "Qty Shipped",
                    A.Bus_ContactName_Exported,
                    A.Bus_EmailAddress_Exported,
                    A.Cons_EmailAddress_Exported,
                    A.cNotes ,
                    2 as iOrderBy
                    FROM RoyaltyOrderIds_{database_id}_ToBeDropped A
                    INNER JOIN RoyaltyOutput_{database_id}_ToBeDropped B ON A.Orderid= b.Orderid
                     ) as tmp;
                    """
            redshift_cursor.execute(sql_script)
            sql_script = f"""
                    SELECT  Report.CreatedBy,
                    Report.ShippedBy,
                    Report.Orderid,
                    Report.Mailer,
                    Report.cNextMarkOrderNo,
                    Report.PO,
                    Report.OrderDescription,
                    Report.Listname,
                    Report.OrderQty,
                    Report.ShipDate,
                    Report.FieldName,
                    Report."Vendor ID",
                    Report."Qty Shipped",
                    Report."Bus_ContactName_Exported",
                    Report."Bus_EmailAddress_Exported",
                    Report."Cons_EmailAddress_Exported",
                    Report.Notes
                    FROM RoyaltyOutput_ForExport_{database_id}_ToBeDropped Report
                    ORDER BY Report.iOrderBy ASC;
                    """
        else:
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
        orders_df.to_csv(report_path, ',', index=False, header=False)


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

def query_redshift(rs_connection, script):
    redshift_conn_id = Variable.get(rs_connection)
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    logging.info('Querying redshift with script :: ', script)
    redshift_hook.run(script, autocommit=True)

def import_in_redshift(database_id,
                       s3_path,
                       iam):
    # 1150 schema does not follow same schema
    if database_id == 1150:
        script = f"""
            DROP TABLE IF EXISTS RoyaltyOrderIds_{database_id}_ToBeDropped;
            CREATE TABLE RoyaltyOrderIds_{database_id}_ToBeDropped
             (  
                  OrderId varchar(10),	
                  cTableName varchar(50),	
                  CreatedBy varchar(50),
                  ShippedBy varchar(50),
                  Mailer varchar(100),
                  cNextMarkOrderNo varchar(50),
                  PO varchar(50),
                  OrderDescription varchar(50),
                  ListName varchar(50),
                  OrderQty varchar(12),
                  ShipDate varchar(10),
                  cNotes varchar(100),
                  Bus_ContactName_Exported varchar(3),
                  Bus_EmailAddress_Exported varchar(3),
                  Cons_EmailAddress_Exported varchar(3),
                  EmailFlag Varchar(1) NULL
                  
            );
                    --	CreatedBy varchar(50),


            COPY RoyaltyOrderIds_{database_id}_ToBeDropped(
                  OrderId,
                  cTableName,
                  CreatedBy,
                  ShippedBy,
                  Mailer,                      
                  cNextMarkOrderNo,
                  PO,
                  OrderDescription,
                  ListName,
                  OrderQty,
                  ShipDate,
                  cNotes,
                  Bus_ContactName_Exported,
                  Bus_EmailAddress_Exported,
                  Cons_EmailAddress_Exported
            )
            FROM '{s3_path}'
            iam_role '{iam}'
            delimiter '|';
                  --	CreatedBy '|',
            DROP TABLE IF EXISTS  RoyaltyFields_{database_id}_ToBeDropped;
            CREATE TABLE RoyaltyFields_{database_id}_ToBeDropped (cFieldName varchar(100));
            """
    else:
        script = f"""
            DROP TABLE IF EXISTS RoyaltyOrderIds_{database_id}_ToBeDropped;
            CREATE TABLE RoyaltyOrderIds_{database_id}_ToBeDropped
             (  
                  OrderId varchar(10),	
                  cTableName varchar(50),	
                  ContactFlag Varchar(1) NULL,
                  EmailFlag Varchar(1) NULL,
                  ShippedBy varchar(50),
                  Mailer varchar(100),               
                  cNextMarkOrderNo varchar(50),
                  PO varchar(50),
                  OrderDescription varchar(50),
                  ListName varchar(50),
                  OrderQty varchar(12),
                  ShipDate varchar(10),
                  OESSOrderID varchar(30),
                  OESSAccountNumber varchar(30),
                  OESSInvoiceTotal varchar(12),
                  SalesRepName varchar (50),
                  DivisionNumber varchar (50), 
                  cNotes varchar(100)
            );
                    --	CreatedBy varchar(50),


            COPY RoyaltyOrderIds_{database_id}_ToBeDropped(
                  OrderId,
                  cTableName,
                  ContactFlag,  
                  EmailFlag,
                  ShippedBy,
                  Mailer,                      
                  cNextMarkOrderNo,
                  PO,
                  OrderDescription,
                  ListName,
                  OrderQty,
                  ShipDate,
                  OESSOrderID,
                  OESSAccountNumber,
                  OESSInvoiceTotal,
                  SalesRepName,
                  DivisionNumber,
                  cNotes
            )
            FROM '{s3_path}'
            iam_role '{iam}'
            delimiter '|';
                  --	CreatedBy '|',
            DROP TABLE IF EXISTS  RoyaltyFields_{database_id}_ToBeDropped;
            CREATE TABLE RoyaltyFields_{database_id}_ToBeDropped (cFieldName varchar(100));
            """

    return script

def clean_up_redshift(database_id):
    cleanup_script = f"""
    DROP TABLE IF EXISTS RoyaltyOrderIds_{database_id}_ToBeDropped;
    DROP TABLE IF EXISTS RoyaltyFields_{database_id}_ToBeDropped;
    DROP TABLE IF EXISTS RoyaltyOrderWithFields_{database_id}_ToBeDropped;
    DROP TABLE IF EXISTS RoyaltyOutput_{database_id}_ToBeDropped;
    DROP TABLE IF EXISTS RoyaltyOutput_ForExport_{database_id}_ToBeDropped;
    DROP TABLE IF EXISTS TablesList_temp_{database_id};"""

    query_redshift('var-redshift-postgres-conn', cleanup_script)



