--create table for loading
drop table if exists {table_buildlol_1};

create table {table_buildlol_1}(
	id integer not null default 0,
	buildid int not null default 0,
	masterlolid int not null default 0,
	lk_action char(1) not null default '',
	lk_actionmonth1 char(1) not null default '',
	lk_actionmonth2 char(1) not null default '',
	lk_actionnextmonth char(1) not null default '',
	lk_quantitytype char(1) not null default '',
	lk_filetype char(2) not null default '',
	iskipfirstrow boolean not null,
	iisactive boolean not null,
	iusage int not null default 0,
	nturns numeric(18, 2) not null default 0,
	--cdecisionreasoning varchar(100) not null default '',
	cslugdate char(6) not null default '',
	cbatchdatetype char(1) not null default '',
	lk_slugdatetype char(1) not null default '',
	iquantityprevious int not null default 0,
	iquantityrequested int not null default 0,
	iquantityreceiveddp int not null default 0,
	iquantityreceived int not null default 0,
	ddatereceived timestamp null,
	iquantityconverted int not null default 0,
	ddateconverted timestamp null,
	iquantitytotal int not null default 0,
	cbatch_lastfrom char(6) not null default '',
	cbatch_lastto char(6) not null default '',
	cbatch_from char(6) not null default '',
	cbatch_to char(6) not null default '',
	order_no varchar(50) null,
	order_clientpo varchar(20) not null default '',
	--order_selection varchar(8000) not null default '',
	--order_fields varchar(8000) null,
	--order_comments varchar(8000) not null default '',
	--order_notes1 varchar(8000) null,
	--order_notes2 varchar(8000) null,
	lk_emailtemplate char(1) null,
	ddateordersent timestamp null,
	--cnotes text not null default '',
	icasapprovalto int not null default 0,
	csourcefilenamereadytoload varchar(300) null,
	csystemfilenamereadytoload varchar(200) not null default '',
	lk_loadfiletype char(3) null,
	lk_loadfilerowterminator char(4) null,
	conepassfilename varchar(50) null,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null,
	--csql varchar(8000) null,
	--csqldescription varchar(8000) null,
	iloadqty int not null default 0,
	lk_encoding varchar(20) not null default '')
diststyle all
compound sortkey(id,buildid,masterlolid);

--load pipe delimited text file with header
copy {table_buildlol_1}
from 's3://{s3-internal}{s3-key7}'
iam_role '{iam}'
acceptinvchars
delimiter '|'
timeformat 'Mon DD YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_buildlol_1}
 where id = 0 or buildid = 0 or masterlolid = 0;

/*
CREATE TABLE [dbo].[tblBuildLoL](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[BuildID] [int] NOT NULL,
	[MasterLoLID] [int] NOT NULL,
	[LK_Action] [char](1) NOT NULL,
	[LK_ActionMonth1] [char](1) NOT NULL,
	[LK_ActionMonth2] [char](1) NOT NULL,
	[LK_ActionNextMonth] [char](1) NOT NULL,
	[LK_QuantityType] [char](1) NOT NULL,
	[LK_FileType] [char](2) NOT NULL,
	[iSkipFirstRow] [bit] NOT NULL,
	[iIsActive] [bit] NOT NULL,
	[iUsage] [int] NOT NULL,
	[nTurns] [numeric](18, 2) NOT NULL,
	[cDecisionReasoning] [varchar](100) NOT NULL,
	[cSlugDate] [char](6) NOT NULL,
	[cBatchDateType] [char](1) NOT NULL,
	[LK_SlugDateType] [char](1) NOT NULL,
	[iQuantityPrevious] [int] NOT NULL,
	[iQuantityRequested] [int] NOT NULL,
	[iQuantityReceivedDP] [int] NOT NULL,
	[iQuantityReceived] [int] NOT NULL,
	[dDateReceived] [datetime] NULL,
	[iQuantityConverted] [int] NOT NULL,
	[dDateConverted] [datetime] NULL,
	[iQuantityTotal] [int] NOT NULL,
	[cBatch_LastFROM] [char](6) NOT NULL,
	[cBatch_LastTO] [char](6) NOT NULL,
	[cBatch_FROM] [char](6) NOT NULL,
	[cBatch_TO] [char](6) NOT NULL,
	[Order_No] [varchar](50) NULL,
	[Order_ClientPO] [varchar](20) NOT NULL,
	[Order_Selection] [varchar](8000) NOT NULL,
	[Order_Fields] [varchar](8000) NULL,
	[Order_Comments] [varchar](8000) NOT NULL,
	[Order_Notes1] [varchar](8000) NULL,
	[Order_Notes2] [varchar](8000) NULL,
	[LK_EmailTemplate] [char](1) NULL,
	[dDateOrderSent] [datetime] NULL,
	[cNotes] [text] NOT NULL,
	[iCASApprovalTo] [int] NOT NULL,
	[cSourceFileNameReadyToLoad] [varchar](300) NULL,
	[cSystemFileNameReadyToLoad] [varchar](200) NOT NULL,
	[LK_LoadFileType] [char](3) NULL,
	[LK_LoadFileRowTerminator] [char](4) NULL,
	[cOnePassFileName] [varchar](50) NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[dModifiedDate] [datetime] NULL,
	[cModifiedBy] [varchar](25) NULL,
	[cSQL] [varchar](8000) NULL,
	[cSQLDescription] [varchar](8000) NULL,
	[iLoadQty] [int] NOT NULL,
	[LK_Encoding] [varchar](20) NOT NULL,
 CONSTRAINT [PK_tblListofLists] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblBuildLoL] ADD  DEFAULT ('') FOR [LK_Encoding]
GO

ALTER TABLE [dbo].[tblBuildLoL]  WITH CHECK ADD  CONSTRAINT [FK_tblListofLists_tblBuild] FOREIGN KEY([BuildID])
REFERENCES [dbo].[tblBuild] ([ID])
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[tblBuildLoL] CHECK CONSTRAINT [FK_tblListofLists_tblBuild]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Value will be used in List Conversion service to load sample data. If contain non zero value then only mentioned records should be processed' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'tblBuildLoL', @level2type=N'COLUMN',@level2name=N'iLoadQty'
GO


 */

