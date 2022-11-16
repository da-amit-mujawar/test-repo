/*
Reju Mathew 2021.04.16.
Jira 674

prepare a table with script to call sp_suppress_oohb  in a loop
Select * from Tempdata.dbo.Activeb2b_tobedropped;

*/
DROP TABLE IF EXISTS Tempdata.dbo.Activeb2b_tobedropped;
CREATE TABLE Tempdata.dbo.Activeb2b_tobedropped
(
    DatabaseID int,
    cDatabaseName varchar(100),
    BuildID int,ctablename varchar(30),
    isAws int default 0,
    strsql varchar(500) default '',
    isApplied int default 0
);
INSERT INTO Tempdata.dbo.Activeb2b_tobedropped (databaseid, cDatabaseName, BuildID)
Select databaseid,B.cDatabaseName, Max(A.ID) as BuildID
from DW_Admin.dbo.tblbuild A
inner join DW_Admin.dbo.tblDatabase B on A.databaseid =B.ID
where databaseid in
 (select  configvalues.Value
	from DW_Admin.dbo.tblConfiguration A
	cross apply STRING_SPLIT (cvalue, ',') configvalues
	where A.cdescription ='BuildProcess'
		and a.citem like 'B2B_DATABASES'
		and a.iIsActive = 1
	)
AND a.iIsOnDisk = 1
AND a.iIsReadyToUse = 1
group by databaseid,B.cDatabaseName
order by databaseid;

UPDATE A
SET ctablename = B.cTableName
FROM Tempdata.dbo.Activeb2b_tobedropped A
INNER JOIN DW_Admin.dbo.tblBuildTable b ON a.BuildID = b.BuildID

UPDATE A
SET isAws = 1,
  strsql = 'call sp_suppress_oohb ('''+ a.cTableName + ''',' + CAST (a.databaseid as Varchar(6)) + ', ''EMAILADDRESS'', ''SHA256LOWER'', ''CINCLUDE'' , '''' );'

FROM Tempdata.dbo.Activeb2b_tobedropped A
Where databaseid in (
	Select  distinct DatabaseID
	from DW_Admin.dbo.tblConfiguration
	where cdescription ='BuildProcess' and citem like 'AWS' 	and cValue ='1' and iIsActive = 1)
