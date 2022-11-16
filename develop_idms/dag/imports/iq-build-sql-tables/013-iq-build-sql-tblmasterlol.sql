--create table for loading
drop table if exists {table_masterlol_1};

create table {table_masterlol_1}(
	id integer not null default 0,
	databaseid int not null default 0,
	ownerid int not null default 0,
	managerid int not null default 0,
	lk_permissiontype char(1) not null default '',
	lk_listtype char(1) not null default '',
	lk_productcode char(2) not null default '',
	lk_decisiongroup char(1) not null default '',
	ccode varchar(10) not null default '',
	cdmidwlistnumber varchar(10) null,
	clistname varchar(100) not null default '',
	cmindatacardcode varchar(15) not null default '',
	cnextmarkid varchar(15) not null default '',
	iordercontactid int not null default 0,
	iismultibuyer boolean not null,
	cappeardate char(6) not null default '',
	clastupdatedate char(6) not null default '',
	cremovedate char(6) not null default '',
	isendcasapproval boolean not null,
	demailsentdate date null,
	nbaseprice_postal int not null default 0,
	nbaseprice_telemarketing int not null default 0,
	nnewbaseprice_postal int null,
	nnewbaseprice_telemarketing int null,
	ccas_signature varchar(25) null,
	ccas_ipaddress varchar(25) null,
	ccas_approvedby varchar(25) null,
	iisactive boolean not null,
	iapprovalform boolean null,
	idropduplicates boolean not null,
	ccustomtext1 varchar(100) not null default '',
	ccustomtext2 varchar(100) not null default '',
	ccustomtext3 varchar(100) not null default '',
	ccustomtext4 varchar(100) not null default '',
	ccustomtext5 varchar(100) not null default '',
	ccustomtext6 varchar(100) not null default '',
	ccustomtext7 varchar(100) not null default '',
	ccustomtext8 varchar(100) null,
	ccustomtext9 varchar(100) null,
	ccustomtext10 varchar(100) null,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null,
	iisncoarequired boolean null,
	iisprofanitycheckrequired boolean not null default '0',
	guid varchar(50) null,
	dvaliduptill timestamp null)
diststyle all
compound sortkey(databaseid,id);

--load pipe delimited text file with header
copy {table_masterlol_1}
from 's3://{s3-internal}{s3-key3}'
iam_role '{iam}'
acceptinvchars
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_masterlol_1}
 where id = 0 or databaseid = 0;

--fix invalid characters
update sql_tblmasterlol_new
set clistname = trim(replace(clistname,'?',' '))
where clistname like '%?%';


/* no need to assign guid newid() at this point... function of sqlserver

CREATE TABLE [dbo].[tblMasterLoL](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DatabaseID] [int] NOT NULL,
	[OwnerID] [int] NOT NULL,
	[ManagerID] [int] NOT NULL,
	[LK_PermissionType] [char](1) NOT NULL,
	[LK_ListType] [char](1) NOT NULL,
	[LK_ProductCode] [char](2) NOT NULL,
	[LK_DecisionGroup] [char](1) NOT NULL,
	[cCode] [varchar](10) NOT NULL,
	[cDMIDWListNumber] [varchar](10) NULL,
	[cListName] [varchar](100) NOT NULL,
	[cMINDatacardCode] [varchar](15) NOT NULL,
	[cNextMarkID] [varchar](15) NOT NULL,
	[iOrderContactID] [int] NOT NULL,
	[iIsMultibuyer] [bit] NOT NULL,
	[cAppearDate] [char](6) NOT NULL,
	[cLastUpdateDate] [char](6) NOT NULL,
	[cRemoveDate] [char](6) NOT NULL,
	[iSendCASApproval] [bit] NOT NULL,
	[dEmailSentDate] [date] NULL,
	[nBasePrice_Postal] [int] NOT NULL,
	[nBasePrice_Telemarketing] [int] NOT NULL,
	[nNewBasePrice_Postal] [int] NULL,
	[nNewBasePrice_Telemarketing] [int] NULL,
	[cCAS_Signature] [varchar](25) NULL,
	[cCAS_IPAddress] [varchar](25) NULL,
	[cCAS_ApprovedBy] [varchar](25) NULL,
	[iIsActive] [bit] NOT NULL,
	[iApprovalForm] [bit] NULL,
	[iDropDuplicates] [bit] NOT NULL,
	[cCustomText1] [varchar](100) NOT NULL,
	[cCustomText2] [varchar](100) NOT NULL,
	[cCustomText3] [varchar](100) NOT NULL,
	[cCustomText4] [varchar](100) NOT NULL,
	[cCustomText5] [varchar](100) NOT NULL,
	[cCustomText6] [varchar](100) NOT NULL,
	[cCustomText7] [varchar](100) NOT NULL,
	[cCustomText8] [varchar](100) NULL,
	[cCustomText9] [varchar](100) NULL,
	[cCustomText10] [varchar](100) NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[dModifiedDate] [datetime] NULL,
	[cModifiedBy] [varchar](25) NULL,
	[iIsNCOARequired] [bit] NULL,
	[iIsProfanityCheckRequired] [bit] NOT NULL,
	[GUID] [uniqueidentifier] NULL ,
	[dValidUpTill] [datetime] NULL,
 CONSTRAINT [PK_tblMasterList] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON,
FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblMasterLoL] ADD  CONSTRAINT
[iIsProfanityCheckRequired_iKeyColumn]  DEFAULT ((0)) FOR [iIsProfanityCheckRequired]
GO
ALTER TABLE [dbo].[tblMasterLoL] ADD  CONSTRAINT
[DF_tblMasterLoL_GUID]  DEFAULT (newid()) FOR [GUID]
GO
ALTER TABLE [dbo].[tblMasterLoL]  WITH CHECK ADD  CONSTRAINT
[FK_tblMasterLOL_tblDatabase] FOREIGN KEY([DatabaseID])
REFERENCES [dbo].[tblDatabase] ([ID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[tblMasterLoL] CHECK CONSTRAINT [FK_tblMasterLOL_tblDatabase]
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'' , @level0type=N'SCHEMA',@level0name=N'dbo',
@level1type=N'TABLE',@level1name=N'tblMasterLoL', @level2type=N'COLUMN',@level2name=N'iOrderContactID'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Used to validated DWAP mail links.' ,
@level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'tblMasterLoL', @level2type=N'COLUMN',
@level2name=N'GUID'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'GUID created is valid till this time.' ,
@level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'tblMasterLoL', @level2type=N'COLUMN',
@level2name=N'dValidUpTill'
GO

 */

