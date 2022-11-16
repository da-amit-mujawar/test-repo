DROP TABLE IF EXISTS temp_pcode;

/*
Jayesh removed this load on 12-22-2021 because data + columns don't match with both IQ tables. 

--create table for loading

create table temp_pcode (
	cdshrd varchar(1) not null default '',
	cdpref varchar(1) not null default '',
	cdtitl varchar(45) not null default '',
	cdypcd varchar(5) not null default '',
	cdsico varchar(6) not null default '',
	cdegcd varchar(1) not null default '',
	cdmyad varchar(4) not null default '',
	cdfran varchar(1) not null default '',
	cdprof varchar(1) not null default '',
	cdsicn varchar(6) not null default '',
	cdslem varchar(6) not null default '',
	cdcflg varchar(1) not null default '',
	cdcfrn varchar(1) not null default '',
	cdpert varchar(1) not null default '',
	cdchdt varchar(4) not null default '',
	cdfttl varchar(45) not null default '',
	cdfrdt varchar(4) not null default '',
	cdhist varchar(6) not null default '',
	cdactv varchar(1) not null default '',
	crlf   varchar(2) not null default '')
DISTSTYLE EVEN;

--load fixed width file no header
copy temp_pcode
from 's3://{s3-internal}/monthly_delivery/PCODE'
iam_role '{iam}'
fixedwidth 'cdshrd:1,cdpref:1,cdtitl:45,cdypcd:5,cdsico:6,cdegcd:1,cdmyad:4,cdfran:1,cdprof:1,cdsicn:6,cdslem:6,
cdcflg:1,cdcfrn:1,cdpert:1,cdchdt:4,cdfttl:45,cdfrdt:4,cdhist:6,cdactv:1,crlf:2'
;

--create sql_ iq table (gross) for loading to redshift --26K
drop table if exists sql_tblsiccode_new;

create table sql_tblsiccode_new (
	csiccode varchar(6) not null default '',
	csicdescription varchar(45) not null,
	ctype char(1) not null default '',
	isPrimary int not null default 0)
diststyle all
compound sortkey(csiccode,csicdescription)
;

--create sql table (de-duped) for loading to redshift --12K
drop table if exists tblsiccode_new;

create table tblsiccode_new (
	csiccode varchar(6) not null default '',
	csicdescription varchar(45) not null,
	ctype char(1) not null default '')
diststyle all
sortkey(csiccode)
;

--if cdpref field has ‘+’ or is blank then it is the primary sic description. 26K
insert into sql_tblsiccode_new (csiccode, csicdescription, ctype,isPrimary)
select distinct cdsicn as csiccode,
                cdtitl as csicdescription,
                '6' as ctype,
                (case when(cdpref='+' or cdpref='') then 1 else 0 end) as isprimary
  from temp_pcode
order by cdsicn, cdtitl
;

--get sic6 for iq. here we need distinct sic6 codes for primary descriptions 12 k
insert into tblsiccode_new
select csiccode, max(csicdescription) as csicdescription
  from sql_tblsiccode_new
 where isprimary = 1
group by csiccode
order by csiccode
;

*/
