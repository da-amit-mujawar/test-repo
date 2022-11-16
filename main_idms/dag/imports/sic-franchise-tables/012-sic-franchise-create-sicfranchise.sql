--create work table
drop table if exists sql_tblsicfranchisecode_new;

create table sql_tblsicfranchisecode_new (
	csiccode varchar(6) not null default '',
	csicdescription varchar(45) not null default '',
	cfranchisecode varchar(1) not null default '',
	cconvertedfranchise varchar(3) not null default '',
	cfranchisename varchar(40) not null default '',
	cfranchisetype varchar(6) not null default '',
	ccanadianflag varchar(1) not null default '',
	coldfranchisecode varchar(1) not null default '',
	ctransactiondate varchar(4) not null default '',
	crlf varchar(2) not null default '')
diststyle all
compound sortkey(cSICCode,cSICDescription,cFranchiseCode,cConvertedFranchise,cFranchiseName,cFranchiseType,cCanadianFlag,cOldFranchiseCode)
;

--load fixed width file no header
copy sql_tblsicfranchisecode_new
from 's3://{s3-internal}/monthly_delivery/PFRANTRAN'
iam_role '{iam}'
ACCEPTINVCHARS
fixedwidth 'csiccode:6,csicdescription:45,cfranchisecode:1,cconvertedfranchise:3,cfranchisename:40,
cfranchisetype:6,ccanadianflag:1,coldfranchisecode:1,ctransactiondate:4,crlf:2'
;

