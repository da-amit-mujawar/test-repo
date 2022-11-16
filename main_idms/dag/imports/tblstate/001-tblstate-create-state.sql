--create table for loading to redshift
drop table if exists sql_tblState;

create table sql_tblState
(
    cstatecode varchar(2) null,
	cstate varchar(50) null,
	ccountycode varchar(3) null,
	ccounty  varchar(50) null,
	ccity  varchar(50) null,
	czip varchar(5) null,
	databaseid int default 0)
diststyle all
compound sortkey (cstatecode,czip);

--load load fixed width file no header
copy sql_tblState
from 's3://{s3-internal}/FilesExportedFromIQ/etl_lookupdata/sql_tblState.txt'
iam_role '{iam}';


