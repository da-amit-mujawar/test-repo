from airflow.models import Variable
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *


def activate_build(report_path):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    config = Variable.get('var-db-1333', deserialize_json=True)
    odd_build_id = config['oddbuildid']
    even_build_id = config['evenbuildid']
    user_id = config['updateuserid']
    # Task 1

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        DECLARE @Buildid int, @Buildid_Even int, @Buildid_Odd int, @nCount int

        SET @Buildid_Even= {even_build_id}
        SET @Buildid_Odd= {odd_build_id}

        --set buildid
        --if (DATENAME ( ww , getdate() )  % 2 ) = 0 
        if (datepart ( w , getdate() )  % 2 ) = 0 
            SET @Buildid =  @Buildid_Even  /*Even weeks build id*/
        ELSE  SET @Buildid =@Buildid_Odd   /*Odd weeks build id*/
              
        Print  @Buildid  

        --Deactivate current build, reset to New build
        UPDATE TOP (2) A
            SET iIsOndisk =0 ,
            LK_BuildStatus =10,
            dModifiedDate = GETDATE(),
            cModifiedBy   = '{user_id}' 
        --Select A.*
        FROM tblBuild A
        WHERE A.ID in (@Buildid_Even,@Buildid_Odd) AND A.ID <> @Buildid  
        AND LK_BuildStatus =75
        AND iIsOndisk =1
        AND a.iIsReadyToUse =1
        
        --Activate pending build
        UPDATE TOP (2) A
            SET iIsOndisk =1 ,
            LK_BuildStatus =75,
            dModifiedDate = GETDATE(),
            cModifiedBy   = '{user_id}' 
        --Select A.*
        FROM tblBuild A
        WHERE A.ID = @Buildid  
        AND LK_BuildStatus =70
        AND iIsOndisk =0
        and a.iIsReadyToUse = 1 
            

        --SELECT @@ROWCOUNT;

        DROP TABLE IF EXISTS ##UltimateNewMover_Activate_ToBeDropped;
        CREATE TABLE ##UltimateNewMover_Activate_ToBeDropped (BUILDID int, Databaseid int, cDescription varchar(70), iRecordCount int);

        INSERT INTO ##UltimateNewMover_Activate_ToBeDropped (BUILDID, Databaseid, cDescription, iRecordCount)
        Select A.ID, A.DatabaseID, A.cDescription, iRecordCount 
        FROM tblBuild A
        WHERE a.ID = @Buildid
        and LK_BuildStatus =75 
        and iIsOndisk =1  ;
                        
        
        Select @nCount = count(*) from ##UltimateNewMover_Activate_ToBeDropped
        
        if ( @nCount <>1 )
        BEGIN
            THROW 50000,'No build to activate. Check the Part 1 update. something went wrong',1
        END             
        """
        print(sql_script)

        # deactivate automation CB 2022.01.27
        # sql_cursor.execute(sql_script)

        # Task 2
        sql_script = f"""
        SELECT A.BuildID, A.Databaseid, B.cDatabaseName,  A.cDescription, A.iRecordCount , 'ACTIVATED' as Status
        FROM ##UltimateNewMover_Activate_ToBeDropped  A
        INNER JOIN dw_admin.dbo.tbldatabase B on A.Databaseid =B.ID
        """
        print(sql_script)
        orders_df = sqlhook.get_pandas_df(sql_script)
        print(orders_df.to_markdown())
        orders_df.to_csv(report_path, '|', index=False)