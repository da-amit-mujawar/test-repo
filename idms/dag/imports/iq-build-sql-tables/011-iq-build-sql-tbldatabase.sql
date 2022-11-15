--create table for loading
drop table if exists {table_database_1};

create table {table_database_1}(
	id integer not null default 0,
	divisionid int not null default 0,
	lk_databasetype char(1) not null default '',
	cdatabasename varchar(80) not null default '',
	clistfileuploadedpath varchar(200) not null default '',
	clistreadytoloadpath varchar(200) not null default '',
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null,
	lk_accountingdivisioncode varchar(20) not null default '',
	cadministratoremail varchar(80) not null default '')
diststyle all;

--load pipe delimited text file with header
copy {table_database_1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_database_1}
 where id = 0 or divisionid = 0 or trim(cdatabasename) = '';


/*
CREATE TABLE dbo.tblDatabase(
	ID int IDENTITY(1,1) NOT NULL,
	DivisionID int NOT NULL,
	LK_DatabaseType char(1) NOT NULL,
	cDatabaseName varchar(80) NOT NULL,
	cListFileUploadedPath varchar(200) NOT NULL,
	cListReadyToLoadPath varchar(200) NOT NULL,
	dCreatedDate datetime NOT NULL,
	cCreatedBy varchar(25) NOT NULL,
	dModifiedDate datetime NULL,
	cModifiedBy varchar(25) NULL,
	LK_AccountingDivisionCode varchar(20) NOT NULL,
	cAdministratorEmail varchar(80) NULL,
 CONSTRAINT PK_tblList PRIMARY KEY CLUSTERED
(
	ID ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON,
FILLFACTOR = 80) ON PRIMARY
) ON PRIMARY
GO

ALTER TABLE dbo.tblDatabase  WITH CHECK ADD  CONSTRAINT FK_tblDatabase_tblDivision FOREIGN KEY(DivisionID)
REFERENCES dbo.tblDivision (ID)
ON DELETE CASCADE
GO

ALTER TABLE dbo.tblDatabase CHECK CONSTRAINT FK_tblDatabase_tblDivision
GO

 */

