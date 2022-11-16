/* prepare a table of db-scripts for executing updates in a loop
Select * from Tempdata.dbo.dnc_b2c_tables;
*/
--NOTE:
--To Add new DB, please add a new line in #DoNotCallDataBases


DROP TABLE IF EXISTS #DoNotCallDataBases;
    CREATE TABLE #DoNotCallDataBases (
        Databaseid int,
        strUpdatesql varchar(800) );

-- UPDATE SQLs for each DB, APPEND new Dbs here
INSERT INTO #DoNotCallDataBases (Databaseid, strUpdatesql)
    Values (1150, 'UPDATE <tblMain> SET DoNotCallFlag=''Y''  FROM <tblMain> A INNER JOIN EXCLUDE_DONOTCALLFLAG B ON A.cons_phonenumber = B.cPhone  where nvl(A.cons_phonenumber,'''') <> ''''  And nvl(A.DoNotCallFlag,'''') <> ''Y'';')
INSERT INTO #DoNotCallDataBases (Databaseid, strUpdatesql)
    Values (1106, 'UPDATE <tblMain> SET DoNotCallFlag=''Y''  FROM <tblMain> A INNER JOIN EXCLUDE_DONOTCALLFLAG B ON A.Phone = B.cPhone  where nvl(A.Phone,'''') <> '''' and nvl(A.DoNotCallFlag,'''') <> ''Y'';')
INSERT INTO #DoNotCallDataBases (Databaseid, strUpdatesql)
    Values (1267, 'UPDATE <tblMain> SET Do_Not_Call_Flag = ''8''  FROM <tblMain> A INNER JOIN EXCLUDE_DONOTCALLFLAG B ON A.Phone_Number = B.cPhone  where nvl(A.Phone_Number,'''') <> '''' and nvl(A.Do_Not_Call_Flag,'''') <> ''8'';')
INSERT INTO #DoNotCallDataBases (Databaseid, strUpdatesql)
    Values (1267, 'UPDATE <tblMain> SET CellPhone_DoNotCallFlag=''8''  FROM <tblMain> A INNER JOIN EXCLUDE_DONOTCALLFLAG B ON A.CellPhone = B.cPhone  where nvl(A.CellPhone,'''') <> '''' and nvl(A.CellPhone_DoNotCallFlag,'''') <> ''8'';')


--Get most recent build and most recent inactive build from same DB
DROP TABLE IF EXISTS Tempdata.dbo.dnc_b2c_tables;
    CREATE TABLE Tempdata.dbo.dnc_b2c_tables (
        Databaseid int,
        cDatabaseName Varchar(100),
        BuildID int,
        cTableName Varchar(100),
        LK_BuildStatus int,
        strsql varchar(800),
        isAWS int default 0,
        isApplied int default 0);

DECLARE @dbid int
DECLARE @strSQL Varchar(800)
DECLARE MyDBListCursor CURSOR for
  SELECT distinct Databaseid,strUpdatesql from #DoNotCallDataBases

Open MyDBListCursor
Fetch NEXT from MyDBListCursor into @dbid, @strSQL
While @@FETCH_STATUS =0
   BEGIN
        INSERT INTO Tempdata.dbo.dnc_b2c_tables (Databaseid, cDatabaseName,BuildID,cTableName, LK_BuildStatus, strsql)

		SELECT TOP 1 D.ID as DatabaseID, D.cDatabaseName, T.BuildID, T.CTABLENAME AS cTABLENAME, LK_BuildStatus, @strsql
		FROM dw_admin.dbo.tblDatabase D
		INNER JOIN dw_admin.dbo.TBLBUILD B ON D.ID = B.DatabaseID
		INNER JOIN dw_admin.dbo.tblBuildTable T ON B.ID = T.BuildID
		WHERE D.ID in (@dbid)
		AND T.cTableName LIKE '%MAIN%' and LK_BuildStatus =75  /* top active build*/
		AND B.ID IN (SELECT MAX(ID) FROM dw_admin.dbo.tblBuild WHERE B.iIsOnDisk =1 AND B.iIsReadyToUse =1 and LK_BuildStatus =75 AND DatabaseID = @dbid)

		Union all

		SELECT TOP 1 D.ID as DatabaseID, D.cDatabaseName, T.BuildID, T.CTABLENAME AS TABLENAME,LK_BuildStatus, @strsql
		FROM dw_admin.dbo.tblDatabase D
		INNER JOIN dw_admin.dbo.TBLBUILD B ON D.ID = B.DatabaseID
		INNER JOIN dw_admin.dbo.tblBuildTable T ON B.ID = T.BuildID
		WHERE D.ID in (@dbid)
		AND T.cTableName LIKE '%MAIN%' and LK_BuildStatus =70  /* Add top most inactive build too, just in case any build is ready to activate.*/
		AND B.ID > (SELECT MAX(ID) FROM dw_admin.dbo.tblBuild WHERE iIsReadyToUse =1 and LK_BuildStatus = 75 AND DatabaseID = @dbid)

		Fetch NEXT from MyDBListCursor into @dbid, @strSQL
    END
CLOSE MyDBListCursor
DEALLOCATE MyDBListCursor

-- Replace variables
UPDATE Tempdata.dbo.dnc_b2c_tables
SET   strsql =  REPLACE(strsql, '<tblMain>',cTableName)

UPDATE A
SET isAws = 1
FROM Tempdata.dbo.dnc_b2c_tables A
Where databaseid in (select distinct DatabaseID
                       from DW_Admin.dbo.tblConfiguration
                      where cdescription ='BuildProcess'
                        and citem like 'AWS'
                        and cValue ='1'
                        and iIsActive = 1);

DELETE FROM Tempdata.dbo.dnc_b2c_tables WHERE isAws = 0;