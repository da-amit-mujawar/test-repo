--create table for loading
drop table if exists {table_extbuildtbldb_1};

create table {table_extbuildtbldb_1}(
	id integer not null default 0,
	databaseid int not null default 0,
	buildtableid int not null default 0,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null)
diststyle all
compound sortkey(databaseid,buildtableid);

--load pipe delimited text file with header
copy {table_extbuildtbldb_1}
from 's3://{s3-internal}{s3-key9}'
iam_role '{iam}'
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_extbuildtbldb_1}
 where id = 0 or databaseid = 0 or buildtableid = 0;


/*
CREATE TABLE [dbo].[tblExternalBuildTableDatabase](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DatabaseID] [int] NULL,
	[BuildTableID] [int] NULL,
	[dCreatedDate] [datetime] NULL,
	[cCreatedBy] [varchar](25) NULL,
	[dModifiedDate] [datetime] NULL,
	[cModifiedBy] [varchar](25) NULL,
 CONSTRAINT [Pk_ExtTableID] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

 */

