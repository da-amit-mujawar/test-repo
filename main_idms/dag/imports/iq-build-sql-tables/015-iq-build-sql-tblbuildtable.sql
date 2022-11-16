--create table for loading
drop table if exists {table_buildtable_1};

create table {table_buildtable_1}(
	id integer not null default 0,
	buildid int not null default 0,
	ctablename varchar(50) not null default '',
	lk_tabletype char(1) not null default '',
	lk_jointype char(1) not null default '',
	cjoinon varchar(50) not null default '',
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null,
	ctabledescription varchar(50) not null default '')
diststyle all
compound sortkey(buildid,id);

--load pipe delimited text file with header
copy {table_buildtable_1}
from 's3://{s3-internal}{s3-key5}'
iam_role '{iam}'
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_buildtable_1}
 where id = 0 or buildid = 0;


/*
CREATE TABLE [dbo].[tblBuildTable](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[BuildID] [int] NOT NULL,
	[cTableName] [varchar](50) NOT NULL,
	[LK_TableType] [char](1) NOT NULL,
	[LK_JoinType] [char](1) NOT NULL,
	[cJoinOn] [varchar](50) NOT NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[dModifiedDate] [datetime] NULL,
	[cModifiedBy] [varchar](25) NULL,
	[ctabledescription] [varchar](50) NOT NULL,
 CONSTRAINT [PK_tblBuildTable] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON,
 FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblBuildTable]  WITH CHECK ADD  CONSTRAINT [FK_tblBuildTable_tblBuild] FOREIGN KEY([BuildID])
REFERENCES [dbo].[tblBuild] ([ID])
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[tblBuildTable] CHECK CONSTRAINT [FK_tblBuildTable_tblBuild]
GO

 */

