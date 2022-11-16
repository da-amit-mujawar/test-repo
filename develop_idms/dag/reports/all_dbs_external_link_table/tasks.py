from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp


def altering_views_for_pub2():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        sql_script = f"""
        DROP VIEW IF EXISTS vwPub2External31_191_201206;
        CREATE VIEW vwPub2External31_191_201206
        AS
        SELECT * FROM tblExternal31_191_201206
        WHERE PUB2SOURCE = 'Y';
        """
        print(sql_script)
        redshift_cursor.execute(sql_script)


def initializing_staging_tables():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        sql_script = f"""
        DROP TABLE IF EXISTS CurrentBuildID_ToBeDropped ;

        SELECT DatabaseId,
            max(id) BuildId INTO CurrentBuildID_ToBeDropped
        FROM SQL_tblBuild
        WHERE iIsOnDisk=1
        GROUP BY DatabaseId;

        DROP TABLE IF EXISTS TableNames_ToBeDropped;
        DROP TABLE IF EXISTS DatabaseName_ToBeDropped;

        SELECT D.ID AS DatabaseID,
            D.cDatabaseName AS DatabaseNAme,
            C.BuildID AS BuildID INTO DatabaseName_ToBeDropped
        FROM SQL_tblDatabase D
        INNER JOIN CurrentBuildID_ToBeDropped C ON D.ID = C.DatabaseID
        WHERE d.id<>848; --remember to remove filter

        SELECT BT.cTableName AS TableName,
            C.BuildId AS BuildID INTO TableNames_ToBeDropped
        FROM SQL_tblBuildTable BT
        INNER JOIN CurrentBuildID_ToBeDropped C ON BT.BuildID = C.BuildID
        WHERE BT.LK_TableType ='M';

        DROP TABLE IF EXISTS ExternalTablesToUse_DONOTDROP;

        CREATE TABLE ExternalTablesToUse_DONOTDROP (
        BuildTableID INT,
        ExternalTableName VARCHAR(50),
        ExternalTableDesc VARCHAR(100),
        JoinColumnName VARCHAR(50),
        ExternalAlias varchar(50),
        Requested varchar(1));

        INSERT INTO ExternalTablesToUse_DONOTDROP (BuildTableID, ExternalTableName, ExternalTableDesc, JoinColumnName, ExternalAlias, Requested)
        VALUES (302,'tblExternal2_191_201206',
                'Experian with LEMS','LEMS','EXPERIAN','Y'),
            (10470,'tblExternal39_191_201206',
             'Apogee Link Table (APG)','LEMS','Apogee Link','Y'),
            (202,'tblExternal1_191_201206','mGen with LEMS','LEMS','MGEN','Y'),
            (780,'tblExternal13_191_201206','InfoGroup - US Business File',
             'Company_MC','InfoGroup - US Business','Y'),
            (781,'tblExternal14_191_201206','InfoGroup - Canadian Business File',
             'Company_MC','InfoGroup - Canadian Business','Y'),
            (1637,'tblExternal15_191_201206','DQI','LEMS','DQI HH','Y'),
            (1690,'tblExternal29_191_201206 ','D&B US Business Database',
             'COMPANY_MC','D&B US Business Database','Y'),
            (1752,'tblExternal30_191_201206','Harte Hanks  US table',
             'COMPANY_MC','Harte Hanks  US','Y'),
            (2575,'tblExternal31_191_201206',
             'Publisher 2 List Rental - LEMS (PB2)','LEMS','PUBLISHER2','Y'),
            (5985,'tblExternal36_191_201206','DQI - Combined Child Table',
             'LEMS','DQI Combined with TargetReady','Y'),
            (6252,'tblExternal37_191_201206',
             'MGEN - Deluxe Lifestyle Enhancements','LEMS','MGEN DELUXE','Y'),
            (8773, 'tblExternal38_191_201206','DQI Individual - Combined',
             'INDIVIDUAL_MC', 'DQI Individual', 'Y'),
            (34493, 'tblExternal42_191_201206',
             'Auto Link', 'LEMS', 'Auto Link', 'Y'),
            (42805, 'tblExternal44_191_201206',
             'Haystaq For Selections ', 'Individual_ID', 'Haystaq', 'Y');

        DROP TABLE IF EXISTS TablesAndExternalLinks_ToBeDropped;

        SELECT A.DatabaseID,
            B.BuildID,
            TN.TableName,
            B.DatabaseName,
            A.BuildTableID AS ExternalTableID,
            ET.ExternalTableName,
            ET.ExternalTableDesc,
            ET.JoinColumnName INTO TablesAndExternalLinks_ToBeDropped
        FROM SQL_tblExternalBuildTableDatabase A
        INNER JOIN DatabaseName_ToBeDropped B ON A.DatabaseID = B.DatabaseID
        INNER JOIN CurrentBuildID_ToBeDropped CD ON B.BuildId = CD.BuildId
        INNER JOIN ExternalTablesToUse_DONOTDROP ET ON ET.BuildTableId = A.BuildTableID
        INNER JOIN TableNames_ToBeDropped TN ON TN.BuildID = B.BuildID;

        ALTER TABLE TablesAndExternalLinks_ToBeDropped 
        ADD column Processed CHAR(1) 
        DEFAULT 'N';

        ALTER TABLE TablesAndExternalLinks_ToBeDropped 
        ADD column
        ProcessedTime datetime;

        INSERT INTO TablesAndExternalLinks_ToBeDropped (DatabaseID, BuildID, TableName, DatabaseName, ExternalTableID, ExternalTableName, ExternalTableDesc, JoinColumnName, Processed)
        SELECT DatabaseID,
            BuildID,
            TableName,
            DatabaseName,
            ExternalTableID,
            'vwPub2External31_191_201206',
            'Publisher 2 List Rental - LEMS (PB2)',
            JoinColumnName,
            Processed
        FROM TablesAndExternalLinks_ToBeDropped
        WHERE ExternalTableName = 'tblExternal31_191_201206';

        DELETE
        FROM TablesAndExternalLinks_ToBeDropped
        WHERE ExternalTableName = 'tblExternal31_191_201206';
        """
        print(sql_script)
        redshift_cursor.execute(sql_script)


def processing_count():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        sql_script = """
        DROP TABLE IF EXISTS TablesandExternalLinkOutputReport;
        CREATE TABLE TablesandExternalLinkOutputReport
        (DatabaseId varchar(10),
        TableName VARCHAR(50),
        ExternalTable varchar(50),
        GrossDatabaseCount BigInt,
        NetDatabaseCount BigInt,
        GrossMatches BigInt,
        DistinctMatchesCounts BigInt,
        Explanation varchar(100),
        JoinOn VARCHAR(50)
        );

        DROP TABLE IF EXISTS DBCount_ToBeDropped;
        CREATE TABLE DBCount_ToBeDropped(
        TableName varchar(100),
        JoinOn varchar(50),
        Counts bigint,
        CountDistinct int);

        DROP TABLE IF EXISTS ExtJoinTableName_ToBeDropped;
        SELECT DISTINCT TableName,Upper(JoinColumnName) AS JoinColumnName,
               '' AS Processed
        INTO ExtJoinTableName_ToBeDropped
        FROM TablesAndExternalLinks_ToBeDropped;
        """
        print(sql_script)
        redshift_cursor.execute(sql_script)
        sql_count = """
        SELECT Count(*) FROM TablesAndExternalLinks_ToBeDropped
        """
        print(sql_count)
        result = redshift_hook.get_first(sql_count)
        total_records = int(result[0])
        print(f'total_records={total_records}')
        position = 1
        while position <= total_records:
            sql_script = """
            SELECT TOP 1 DatabaseID,
            TableName,
            ExternalTableName,
            JoinColumnName
            FROM TablesAndExternalLinks_ToBeDropped
            WHERE processed='N'
            ORDER BY DatabaseID,ExternalTableName ASC
            """
            print(sql_script)
            redshift_cursor.execute(sql_script)
            row = redshift_cursor.fetchone()
            database_id, table_name, external_table, join_on = row[0], row[1], row[2], row[3]
            print(
                f'database_id={database_id},table_name={table_name},external_table={external_table},join_on={join_on}')
            base_insert_query = f"""
            INSERT INTO TablesandExternalLinkOutputReport(
                DatabaseId, TableName, ExternalTable, GrossDatabaseCount,
                GrossMatches, DistinctMatchesCounts, Explanation, JoinOn)
            VALUES ('{database_id}','{table_name}','{external_table}',0,0,0
            """
            update_script = f"""
            UPDATE TablesAndExternalLinks_ToBeDropped
                SET processed='X'
                WHERE DatabaseID={database_id}
                AND TableName='{table_name}'
                AND ExternalTableName='{external_table}'
                AND processed='N'
            """
            # 1
            sql_script = f"""
            Select NOT EXISTS (select top 1 * from information_schema.columns where table_name='{table_name}')
            """
            print(sql_script)
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                sql_script = f"""
                {base_insert_query},'{table_name} not found in Redshift','{join_on}');
                --1st condition
                {update_script}
                """
                print(sql_script)
                redshift_hook.run(sql_script)
            # 2
            sql_script = f"""
            Select NOT EXISTS (select top 1 * from information_schema.columns where table_name='{external_table}')
            """
            print(sql_script)
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                sql_script = f"""
                {base_insert_query},'{external_table} not found in Redshift','{join_on}');
                --2nd condition
                {update_script}
                """
                print(sql_script)
                redshift_hook.run(sql_script)
            # 3
            sql_script = f"""
            Select NOT EXISTS (select lower(table_name) AS table_name from information_schema.columns where lower(table_name) = lower('{table_name}')  and lower(column_name)=lower('{join_on}'))
            """
            print(sql_script)
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                sql_script = f"""
                {base_insert_query},'{join_on} is not in {table_name}','{join_on}');
                --3rd
                {update_script}
                """
                print(sql_script)
                redshift_hook.run(sql_script)
            # 4
            sql_script = f"""
            Select NOT EXISTS (select lower(table_name) AS table_name from information_schema.columns where lower(table_name) = lower('{external_table}')  and lower(column_name)=lower('{join_on}'))
            """
            print(sql_script)
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                sql_script = f"""
                {base_insert_query},'{join_on} is not in {external_table}','{join_on}');
                --4th
                {update_script}
                """
                print(sql_script)
                redshift_hook.run(sql_script)
            # 5
            sql_script = f"""
            Select EXISTS (select top 1 * from information_schema.tables where table_name='{table_name}')
                AND EXISTS (select top 1 * from information_schema.tables where table_name ='{external_table}')
            """
            print(sql_script)
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                insert_script = f"""
                INSERT INTO TablesandExternalLinkOutputReport(
                    DatabaseID,
                    TableName,
                    ExternalTable,
                    GrossMatches,
                    DistinctMatchesCounts,
                    JoinOn)
                SELECT  '{database_id}' , '{table_name}' , '{external_table}'
                Count(A.{join_on}) GrossMatches,Count(Distinct A.{join_on}) DistinctMatchesCounts,'{join_on}'
                FROM {table_name} A
                INNER JOIN {external_table} B on A.{join_on}=B.{join_on}
                """
                print(insert_script)
                redshift_hook.run(insert_script)
            # 6 Update
            update_script = f"""
               -- 6th condition.
               UPDATE TablesAndExternalLinks_ToBeDropped
               SET processed='Y',
               ProcessedTime=getdate()
               WHERE DatabaseID={database_id}
               AND TableName='{table_name}'
               AND ExternalTableName='{external_table}'
               AND processed='N' 
               """
            print(update_script)
            redshift_hook.run(update_script)
            # 7 Increment position
            position = position + 1
        sql_count = """
        SELECT Count(*) FROM ExtJoinTableName_ToBeDropped
        """
        print(sql_count)
        result = redshift_hook.get_first(sql_count)
        total_records = int(result[0])
        print(f'total_records={total_records}')
        position = 1
        while position <= total_records:
            sql_script = """
            SELECT TOP 1 TableName,
            JoinColumnName
            FROM ExtJoinTableName_ToBeDropped
            WHERE processed=''
            """
            print(sql_script)
            redshift_cursor.execute(sql_script)
            row = redshift_cursor.fetchone()
            table_name, join_on = row[0], row[1]
            print(f'table_name={table_name},join_on={join_on}')
            sql_script = f"""
            Select EXISTS (select lower(table_name) AS table_name, LOWER(column_name) AS column_name from information_schema.columns where lower(table_name) = lower('{table_name}')  and lower(column_name)=lower('{join_on}'))
            """
            print(sql_script)
            result = redshift_hook.get_first(sql_script)
            if result[0]:
                insert_script = f"""
                INSERT INTO DBCount_ToBeDropped(
                    TableName,
                    JoinOn,
                    Counts,
                    CountDistinct)
                    SELECT '{table_name}' TableName, '{join_on}' JoinOn, Count({join_on}) Counts, COUNT(DISTINCT {join_on}) CountDistinct 
                    FROM {table_name};

                    UPDATE ExtJoinTableName_ToBeDropped  SET processed='Y' 
                    WHERE TableName='{table_name}' 
                    AND JoinColumnName='{join_on}'
                    AND processed='';  
                """
                print(insert_script)
                redshift_hook.run(insert_script)
            else:
                update_script = f"""
                -- 2nd Loop Update
                UPDATE ExtJoinTableName_ToBeDropped  
                SET processed='X' 
                WHERE TableName='{table_name}' 
                AND JoinColumnName='{join_on}'
                AND processed='';
                """
                print(update_script)
                redshift_hook.run(update_script)
            position = position + 1
        sql_script = """
        -- Reamining Script for Task#3 
        UPDATE TablesandExternalLinkOutputReport
        SET GrossDatabaseCount = B.Counts,
           NetDatabaseCount = B.CountDistinct
        FROM TablesandExternalLinkOutputReport A
        INNER JOIN DBCount_ToBeDropped B
        ON A.TableName = B.TableName
        AND UPPER(A.JoinOn) = UPPER(B.JoinOn);
        """
        print(sql_script)
        redshift_cursor.execute(sql_script)


def report_generation():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        sql_script = f"""
        
        DROP TABLE IF EXISTS ExternalLinkTableReport_ToBeDropped;

        SELECT TableName,ExternalTable,
        MAX(CASE WHEN ExternalTable = 'tblExternal1_191_201206' THEN DistinctMatchesCounts ELSE 0 END) MGEN,
        MAX(CASE WHEN ExternalTable = 'tblExternal2_191_201206' THEN DistinctMatchesCounts ELSE 0 END) EXPERIAN,
        MAX(CASE WHEN ExternalTable = 'tblExternal13_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "InfoGroup - US Business",
        MAX(CASE WHEN ExternalTable = 'tblExternal14_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "InfoGroup - Canadian Business",
        MAX(CASE WHEN ExternalTable = 'tblExternal15_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "DQI HH",
        MAX(CASE WHEN ExternalTable = 'tblExternal29_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "D&B US Business Database",
        MAX(CASE WHEN ExternalTable = 'tblExternal30_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "Harte Hanks  US",
        --Removed. Susan 2018.12.19 MAX(CASE WHEN ExternalTable = 'vwPub1External31_191_201206' THEN DistinctMatchesCounts ELSE 0 END) PUBLISHER1,
        MAX(CASE WHEN ExternalTable = 'vwPub2External31_191_201206' THEN DistinctMatchesCounts ELSE 0 END) PUBLISHER2,
        MAX(CASE WHEN ExternalTable = 'tblExternal36_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "DQI Combined with TargetReady",
        MAX(CASE WHEN ExternalTable = 'tblExternal37_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "MGEN DELUXE",
        MAX(CASE WHEN ExternalTable = 'tblExternal39_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "Apogee Link",
        /*Added by SF on Jan 28, 2019 per ticket 747209 */
        MAX(CASE WHEN ExternalTable = 'tblExternal38_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "DQI Individual",
        MAX(CASE WHEN ExternalTable = 'tblExternal42_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "Auto Link",
        /*Added by SF on 08.15.2019 per ticket 799094 */
        MAX(CASE WHEN ExternalTable = 'tblExternal43_191_201206' THEN DistinctMatchesCounts ELSE 0 END) "Intent Database",
        MAX(CASE WHEN ExternalTable = 'tblExternal44_191_201206' THEN DistinctMatchesCounts ELSE 0 END) Haystaq

        INTO ExternalLinkTableReport_ToBeDropped 
        FROM TablesandExternalLinkOutputReport
        GROUP BY TableName,ExternalTable;

              
        DROP TABLE IF EXISTS Final_ExternalLinkTableReport;

        SELECT DISTINCT C.DatabaseName,
        C.DatabaseID AS DBID,
        B.GrossDatabaseCount,
        B.NetDatabaseCount,
        UPPER(B.JoinOn) AS JoinOn,
        A.MGEN,
        EXPERIAN,
        A."InfoGroup - US Business",
        A."InfoGroup - Canadian Business",
        A."DQI HH",
        A."D&B US Business Database",
        A."Harte Hanks  US",
        A.PUBLISHER2,
        A."DQI Combined with TargetReady",
        A."MGEN DELUXE",
        A."Apogee Link",
        A."DQI Individual",
        A."Auto Link",
        A."Intent Database",
        A.Haystaq INTO Final_ExternalLinkTableReport
        FROM ExternalLinkTableReport_ToBeDropped A
        INNER JOIN TablesandExternalLinkOutputReport B ON A.TableName = B.TableName
        AND A.ExternalTable = B.ExternalTable
        INNER JOIN DatabaseName_ToBeDropped C ON CAST(B.DatabaseID AS INT) = C.DatabaseID;

        DROP TABLE IF EXISTS FinalReport_ToBeDropped;
        SELECT A.DatabaseName,A.DBID,
            MAX(A.GrossDatabaseCount) AS GrossDatabaseCount,
            MAX(A.NetDatabaseCount) AS NetDatabaseCount,
            A.JoinOn,
            MAX(A."DQI Individual") AS "DQI Individual",  /*Added By SF on Jan 28, 2019 per ticket 747209 */
            MAX(A."DQI HH") AS "DQI HH",
            MAX(A.MGEN) AS MGEN,
            MAX(A."MGEN DELUXE") AS "MGEN DELUXE",
            MAX(A."Apogee Link") AS "Apogee Link",       
            MAX(A.EXPERIAN) AS EXPERIAN,
            MAX(A.PUBLISHER2) AS PUBLISHER2,       
            MAX(A."DQI Combined with TargetReady") AS "TargetReady",
            MAX(A."Auto Link") AS "Auto Link",  /*Added By SF on Jan 28, 2019 per ticket 747209 */
            MAX(A."InfoGroup - US Business") AS "IG - US Business",
            MAX(A."InfoGroup - Canadian Business") AS "IG - Canadian Business",
            MAX(A."D&B US Business Database") AS "D&B US Business Database",
            MAX(A."Harte Hanks  US") AS "Harte Hanks  US",
            MAX(A."Intent Database") AS "Intent Database", /*Added By SF on Jan 28, 2019 per ticket 747209 */
            MAX(A.Haystaq) AS Haystaq   /*Added By SF on Jan 28, 2019 per ticket 747209 */                        
        INTO FinalReport_ToBeDropped 
        FROM Final_ExternalLinkTableReport A
        GROUP BY DatabaseName,JoinOn,DBID
        ORDER BY DATABASENAME; 

        """
        print(sql_script)
        redshift_cursor.execute(sql_script)
