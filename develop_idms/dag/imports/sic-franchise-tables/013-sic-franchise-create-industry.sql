--create work table
drop table if exists tblindustrycode_new;

create table tblindustrycode_new (
    csiccode varchar(6) not null default '',
    cindustryspecificcode varchar(1) not null default '',
    cpositionindicator varchar(1) not null default '',
    cindustryspecificdescription varchar(40) not null default '',
    crangefromvalue varchar(9) not null default '',
    crangetovalue varchar(9) not null default '')
diststyle all
compound sortkey(csiccode,cindustryspecificcode,cpositionindicator)
;

--create work table
drop table if exists sql_tblindustrycode_new;

create table sql_tblindustrycode_new (
    csiccode varchar(6) not null default '',
    cindustryspecificcode varchar(1) not null default '',
    cpositionindicator varchar(1) not null default '',
    cindustryspecificdescription varchar(40) not null default '',
    crangefromvalue varchar(9) not null default '',
    crangetovalue varchar(9) not null default '')
diststyle all
compound sortkey(csiccode,cindustryspecificcode,cpositionindicator)
;

--load fixed width file no header
copy tblindustrycode_new
from 's3://{s3-internal}/monthly_delivery/indmastr'
iam_role '{iam}'
fixedwidth 'csiccode:6,cindustryspecificcode:1,cpositionindicator:1,cindustryspecificdescription:40,crangefromvalue:9,
crangetovalue:9'
;

--load fixed width file no header
copy sql_tblindustrycode_new
from 's3://{s3-internal}/monthly_delivery/indmastr'
iam_role '{iam}'
fixedwidth 'csiccode:6,cindustryspecificcode:1,cpositionindicator:1,cindustryspecificdescription:40,crangefromvalue:9,
crangetovalue:9'
;


-- remove empty values in join columns
delete from tblindustrycode_new where csiccode = '' or cindustryspecificcode = '';
delete from sql_tblindustrycode_new  where csiccode = '' or cindustryspecificcode = '';

--these are updates to dw_admin tables, not IQ...
/*--per jayesh, do not apply
update {dw_admin.dbo.tblSICCode}
   set cindicator = 'F'
 where csiccode in (select distinct csiccode from {table_iq_fran_1})
   and cindicator = '';

update {dw_admin.dbo.tblSICCode}
   set cindicator = 'I'
 where csiccode in (select distinct csiccode from {table_iq_industry_1})
   and cindicator = '';

update {dw_admin.dbo.tblSICCode}
   set cindicator = 'B'
 where csiccode in (select distinct csiccode from {table_iq_industry_1})
   and cindicator = 'F';

update {dw_admin.dbo.tblSICCode}
   set cindicator = 'B'
 where csiccode in (select distinct csiccode from {table_iq_fran_1})
   and cindicator = 'I';
*/

/* This was one time load as part of GUI migration.
   If it is not in IQ currently, no need to create it in RedShift.
update dw_admin.dbo.tblsiccoderelated
set cindicator =
case when (sic.[cindicator] is null) then '' else sic.[cindicator] end
from [dw_admin].[dbo].[tblsiccoderelated] relatedsic
left join [dw_admin].[dbo].[tblsiccode] sic on relatedsic.[crelatedsiccode]=sic.[csiccode]
and relatedsic.[crelatedsicdescription]=sic.[csicdescription]
*/

