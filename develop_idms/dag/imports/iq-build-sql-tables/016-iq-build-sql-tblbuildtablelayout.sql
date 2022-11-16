--create table for loading
drop table if exists {table_buildtablelayout_1};

create table {table_buildtablelayout_1}(
	id integer not null default 0,
	buildtableid int not null default 0,
	cfieldname varchar(50) not null default '',
	cfielddescription varchar(50) not null default '',
	cdatatype varchar(50) not null default '',
	idatalength int not null default 0,
	iismailerspecific boolean not null,
	iissystem boolean not null,
	iislistspecific boolean not null,
	iisnormalized boolean not null,
	iisenhancement boolean null,
	iismappingrequired boolean not null,
	iismappingallowed boolean not null,
	cnormfieldname varchar(50) not null default '',
	iaudittype int not null default 0,
	iisselectable boolean not null,
	iisbillable boolean not null,
	ishowlistbox boolean not null,
	ishowtextbox boolean not null,
	ifileoperations boolean not null,
	ishowdefault int not null default 0,
	iallowsorting boolean not null,
	iallowmaxper boolean not null,
	ilayoutorder int not null default 0,
	iallowexport boolean not null,
	idisplayorder int not null default 0,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null,
	cfieldtype char(1) not null default 'S',
	lk_indextype varchar(2) not null default '',
	ikeycolumn int null,
	iisrebuilddd boolean not null)
diststyle even
compound sortkey (buildtableid,cfieldname);

--load pipe delimited text file with no header
copy sql_tblbuildtablelayout_new
from 's3://{s3-internal}{s3-key6}'
iam_role '{iam}'
acceptinvchars
delimiter '|'
timeformat 'Mon DD YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_buildtablelayout_1}
 where id = 0 or buildtableid = 0;


/*
CREATE TABLE [dbo].[tblBuildTableLayout](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[BuildTableID] [int] NOT NULL,
	[cFieldName] [varchar](50) NOT NULL,
	[cFieldDescription] [varchar](50) NOT NULL,
	[cDataType] [varchar](50) NOT NULL,
	[iDataLength] [int] NOT NULL,
	[iIsMailerSpecific] [bit] NOT NULL,
	[iIsSystem] [bit] NOT NULL,
	[iIsListSpecific] [bit] NOT NULL,
	[iIsNormalized] [bit] NOT NULL,
	[iIsEnhancement] [bit] NULL,
	[iIsMappingRequired] [bit] NOT NULL,
	[iIsMappingAllowed] [bit] NOT NULL,
	[cNormFieldName] [varchar](50) NOT NULL,
	[iAuditType] [int] NOT NULL,
	[iIsSelectable] [bit] NOT NULL,
	[iIsBillable] [bit] NOT NULL,
	[iShowListBox] [bit] NOT NULL,
	[iShowTextBox] [bit] NOT NULL,
	[iFileOperations] [bit] NOT NULL,
	[iShowDefault] [int] NOT NULL,
	[iAllowSorting] [bit] NOT NULL,
	[iAllowMaxPer] [bit] NOT NULL,
	[iLayoutOrder] [int] NOT NULL,
	[iAllowExport] [bit] NOT NULL,
	[iDisplayOrder] [int] NOT NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[dModifiedDate] [datetime] NULL,
	[cModifiedBy] [varchar](25) NULL,
	[cFieldType] [char](1) NOT NULL,
	[LK_IndexType] [varchar](2) NOT NULL,
	[iKeyColumn] [int] NULL,
	[iIsRebuildDD] [bit] NOT NULL,
 CONSTRAINT [PK_tblLayout] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON,
 FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblBuildTableLayout] ADD  DEFAULT ('S') FOR [cFieldType]
GO

ALTER TABLE [dbo].[tblBuildTableLayout]  WITH CHECK ADD  CONSTRAINT
[FK_tblLayout_tblBuildTable] FOREIGN KEY([BuildTableID]) REFERENCES [dbo].[tblBuildTable] ([ID])
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[tblBuildTableLayout] CHECK CONSTRAINT [FK_tblLayout_tblBuildTable]
GO

 */

