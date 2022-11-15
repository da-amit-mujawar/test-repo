--create table for loading
drop table if exists {table_owner_1};

create table {table_owner_1}(
	id integer not null default 0,
	databaseid int not null default 0,
	ccode char(15) not null default '',
	ccompany varchar(50) not null default '',
	caddress1 varchar(50) not null default '',
	caddress2 varchar(50) not null default '',
	ccity varchar(30) not null default '',
	cstate char(2) not null default '',
	czip varchar(10) not null default '',
	cphone varchar(20) not null default '',
	cfax varchar(20) not null default '',
	cnotes varchar(32767) not null default '',
	iisactive boolean not null,
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	cmodifiedby varchar(25) null,
	dmodifieddate timestamp null)
diststyle all
compound sortkey (databaseid,id);

--load pipe delimited text file with header
copy {table_owner_1}
from 's3://{s3-internal}{s3-key2}'
iam_role '{iam}'
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_owner_1}
 where id = 0 or databaseid = 0;


/*

CREATE TABLE [dbo].[tblOwner](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DatabaseID] [int] NOT NULL,
	[cCode] [char](15) NOT NULL,
	[cCompany] [varchar](50) NOT NULL,
	[cAddress1] [varchar](50) NOT NULL,
	[cAddress2] [varchar](50) NOT NULL,
	[cCity] [varchar](30) NOT NULL,
	[cState] [char](2) NOT NULL,
	[cZip] [varchar](10) NOT NULL,
	[cPhone] [varchar](20) NOT NULL,
	[cFax] [varchar](20) NOT NULL,
	[cNotes] [text] NOT NULL,
	[iIsActive] [bit] NOT NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[cModifiedBy] [varchar](25) NULL,
	[dModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_tblOwner] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON,
 FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblOwner]  WITH CHECK ADD  CONSTRAINT [FK_tblOwner_tblDatabase] FOREIGN KEY([DatabaseID])
REFERENCES [dbo].[tblDatabase] ([ID])
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[tblOwner] CHECK CONSTRAINT [FK_tblOwner_tblDatabase]
GO


 */

