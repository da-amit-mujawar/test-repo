
DROP TABLE IF EXISTS Tempdata.dbo.eng_b2b_tables;
    CREATE TABLE Tempdata.dbo.eng_b2b_tables
    (
        DatabaseID int,
        cDatabaseName varchar(100),
        BuildID int,
        ctablename varchar(30),
        isAws int default 0,
        strsql varchar(500) default '',
        isApplied int default 0
    );

INSERT INTO Tempdata.dbo.eng_b2b_tables (databaseid, cDatabaseName, BuildID)
    Select databaseid,B.cDatabaseName, Max(A.ID) as BuildID
    from DW_Admin.dbo.tblbuild A
    inner join DW_Admin.dbo.tblDatabase B on A.databaseid =B.ID
    where databaseid in (
        select  configvalues.Value
        from DW_Admin.dbo.tblConfiguration A
        cross apply STRING_SPLIT (cvalue, ',') configvalues
        where A.cdescription ='BuildProcess'
            and a.citem like 'B2B_DATABASES'
            and a.iIsActive = 1 )
    AND a.iIsOnDisk = 1
    AND a.iIsReadyToUse = 1
    group by databaseid,B.cDatabaseName
    order by databaseid;

UPDATE A
    SET ctablename = B.cTableName
    FROM Tempdata.dbo.eng_b2b_tables A
    INNER JOIN DW_Admin.dbo.tblBuildTable b ON a.BuildID = b.BuildID and LK_TableType = 'M';

UPDATE A
    SET isAws = 1,
    strsql = 'call sp_apply_engagers ('''+ a.cTableName + ''', ''EMAILADDRESS'' );'
    FROM Tempdata.dbo.eng_b2b_tables A
    Where databaseid in (
        Select  distinct DatabaseID
        from DW_Admin.dbo.tblConfiguration
        where cdescription ='BuildProcess' and citem like 'AWS' and cValue ='1' and iIsActive = 1);

DELETE FROM Tempdata.dbo.eng_b2b_tables WHERE isAws = 0;
