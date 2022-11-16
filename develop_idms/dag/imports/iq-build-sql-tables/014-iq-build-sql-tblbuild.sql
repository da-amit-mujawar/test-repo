--create table for loading
drop table if exists {table_build_1};

create table {table_build_1}(
	id integer not null default 0,
	databaseid int not null default 0,
	lk_buildstatus char(2) null,
	ipreviousbuildid int null,
	cbuild char(6) not null default '',
	cdescription varchar(50) not null default '',
	dmaildate timestamp null,
	irecordcount int not null default 0,
	iisreadytouse boolean not null,
	iisondisk boolean not null,
	cmaildatefrom char(6) null,
	cmaildateto char(6) null,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null,
	lk_buildpriority int not null default 0,
	dscheduleddatetime timestamp null,
	istoprequested boolean not null,
	iisonestep boolean not null)
diststyle all
compound sortkey(databaseid,id);

--load pipe delimited text file with header
copy {table_build_1}
from 's3://{s3-internal}{s3-key4}'
iam_role '{iam}'
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_build_1}
 where id = 0 or databaseid = 0;


/*
CREATE TABLE dbo.tblBuild(
	ID int IDENTITY(1,1) NOT NULL,
	DatabaseID int NOT NULL,
	LK_BuildStatus char(2) NULL,
	iPreviousBuildID int NULL,
	cBuild char(6) NOT NULL,
	cDescription varchar(50) NOT NULL,
	dMailDate datetime NULL,
	iRecordCount int NOT NULL,
	iIsReadyToUse bit NOT NULL,
	iIsOnDisk bit NOT NULL,
	cMailDateFROM char(6) NULL,
	cMailDateTO char(6) NULL,
	dCreatedDate datetime NOT NULL,
	cCreatedBy varchar(25) NOT NULL,
	dModifiedDate datetime NULL,
	cModifiedBy varchar(25) NULL,
	LK_BuildPriority int NOT NULL,
	dScheduledDateTime datetime NULL,
	iStopRequested bit NOT NULL,
	iIsOneStep bit NOT NULL,
 CONSTRAINT PK_tblBuild PRIMARY KEY CLUSTERED
(
	ID ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON,
FILLFACTOR = 80) ON PRIMARY
) ON PRIMARY
GO

ALTER TABLE dbo.tblBuild  WITH CHECK ADD  CONSTRAINT FK_tblBuild_tblList FOREIGN KEY(DatabaseID)
REFERENCES dbo.tblDatabase (ID)
ON DELETE CASCADE
GO

ALTER TABLE dbo.tblBuild CHECK CONSTRAINT FK_tblBuild_tblList
GO


 */

