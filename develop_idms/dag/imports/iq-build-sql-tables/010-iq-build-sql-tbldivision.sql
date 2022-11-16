--create table for loading
drop table if exists {table_division_1};

create table {table_division_1}(
	id integer not null default 0,
    cdivisionname varchar(50) not null default '',
    cdivisiondescription varchar(50) not null default '',
    caddress1 varchar(50) not null default '',
    caddress2 varchar(50) not null default '',
    ccity varchar(30) not null default '',
    cstate char(2) not null default '',
    czip varchar(10) not null default '',
    cphone varchar(20) not null default '',
    cfax varchar(20) not null default '',
    cemail varchar(80) not null default '',
    clogopath varchar(100) not null default '',
    cofferpath varchar(250) not null default '',
    cstatus char(1) not null default '',
    dcreateddate timestamp not null default getdate(),
    ccreatedby varchar(25) not null default '',
    cmodifiedby varchar(25) null,
    dmodifieddate timestamp null)
diststyle all;

--load pipe delimited text file with header
copy {table_division_1}
from 's3://{s3-internal}{s3-key0}'
iam_role '{iam}'
delimiter '|'
timeformat 'MM/DD/YYYY HH:MI:SS';

-- remove empty values in join columns
delete from {table_division_1}
 where id = 0 or trim(cdivisionname) = '';


/*
CREATE TABLE [dbo].[tblDivision](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[cDivisionName] [varchar](50) NOT NULL,
	[cDivisionDescription] [varchar](50) NOT NULL,
	[cAddress1] [varchar](50) NOT NULL,
	[cAddress2] [varchar](50) NOT NULL,
	[cCity] [varchar](30) NOT NULL,
	[cState] [char](2) NOT NULL,
	[cZip] [varchar](10) NOT NULL,
	[cPhone] [varchar](20) NOT NULL,
	[cFax] [varchar](20) NOT NULL,
	[cEmail] [varchar](80) NOT NULL,
	[cLogoPath] [varchar](100) NOT NULL,
	[cOfferPath] [varchar](250) NOT NULL,
	[cStatus] [char](1) NOT NULL,
	[dCreatedDate] [datetime] NOT NULL,
	[cCreatedBy] [varchar](25) NOT NULL,
	[cModifiedBy] [varchar](25) NULL,
	[dModifiedDate] [datetime] NULL,
 CONSTRAINT [PK_tblDivision] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON,
 ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO

 */

