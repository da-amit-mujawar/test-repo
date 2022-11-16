--create table for loading
drop table if exists {table_builddd_1};

create table {table_builddd_1}(
	id integer not null default 0,
	buildtablelayoutid int not null default 0,
	buildlolid int not null default 0,
	cvalue varchar(150) null,
	cdescription varchar(255) not null default '',
	cnormalizedvalue varchar(30) not null default '',
	idisplayorder int not null default 0,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null)
compound sortkey(id,buildtablelayoutid,buildlolid);

--load pipe delimited text file with header
copy {table_builddd_1}
from 's3://{s3-internal}{s3-key8}'
iam_role '{iam}'
acceptinvchars
delimiter '|'
timeformat 'Mon DD YYYY HH:MI:SS';

-- remove empty values in join columns
--delete from {table_builddd_1}
-- where id = 0 or buildtablelayoutid = 0 or buildlolid = 0;

/*
CREATE TABLE [dbo].[tblBuildDD](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[BuildTableLayoutID] [int] NOT NULL,
	[BuildLoLID] [int] NOT NULL,
	[cValue] [varchar](150) NULL,
	[cDescription] [varchar](255) NOT NULL,
	[cNormalizedValue] [varchar](30) NOT NULL,
	[iDisplayOrder] [int] NOT NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[dModifiedDate] [datetime] NULL,
	[cModifiedBy] [varchar](25) NULL,
 CONSTRAINT [PK_tblDataDictionary] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblBuildDD]  WITH CHECK ADD  CONSTRAINT [FK_tblDataDictionary_tblLayout] FOREIGN KEY([BuildTableLayoutID])
REFERENCES [dbo].[tblBuildTableLayout] ([ID])
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[tblBuildDD] CHECK CONSTRAINT [FK_tblDataDictionary_tblLayout]
GO

 */

