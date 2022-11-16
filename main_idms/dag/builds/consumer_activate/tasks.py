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
from helpers.s3 import save_dataframe, delete_file, copy_file


def activate_build():
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)  

    databaseid = 1267
    userName = 'Airflow'

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS Tempdata.dbo.InfoGroupConsumer_Activate_ToBeDropped;
        CREATE TABLE Tempdata.dbo.InfoGroupConsumer_Activate_ToBeDropped (BUILDID int, Databaseid int, cDescription varchar(70), iRecordCount int);

        INSERT INTO Tempdata.dbo.InfoGroupConsumer_Activate_ToBeDropped (BUILDID, Databaseid, cDescription, iRecordCount)
        Select TOP(1) A.ID, A.DatabaseID, A.cDescription, iRecordCount 
        FROM dw_admin.dbo.tblBuild A
        WHERE a.DatabaseID = {databaseid}
        AND a.iIsReadyToUse = 1   
	    AND LK_BuildStatus =70
	    AND cDescription NOT LIKe '%TEST%' 
	    AND cDescription NOT LIKe '%DON%'
	    AND cDescription NOT LIKe '%NOT%'
        AND a.ID > (SELECT MAX(ID) FROM dw_admin.dbo.tblBuild WHERE LK_BuildStatus =75 and iIsOndisk =1  AND DatabaseID = {databaseid})
        Order by ID DESC;
		

        UPDATE TOP (1) A
	    SET iIsOndisk =1 ,
        LK_BuildStatus =75,
   	    dModifiedDate = GETDATE(),
   	    cModifiedBy   = '{userName}'
        FROM dw_admin.dbo.tblBuild A
        INNER JOIN Tempdata.dbo.InfoGroupConsumer_Activate_ToBeDropped B on A.ID = B.BUILDID
        WHERE a.iIsReadyToUse = 1   
        AND LK_BuildStatus =70 ;
       
        """
        print(sql_script)
        sql_cursor.execute(sql_script)       
        
    
     

    
        

