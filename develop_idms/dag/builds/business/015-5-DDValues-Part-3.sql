/*
drop table if exists sql_tblSICFranchiseCode;

create table sql_tblSICFranchiseCode (
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

copy sql_tblSICFranchiseCode
from 's3://idms-7933-internalfiles/monthly_delivery/PFRANTRAN'
iam_role 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
ACCEPTINVCHARS
fixedwidth 'csiccode:6,csicdescription:45,cfranchisecode:1,cconvertedfranchise:3,cfranchisename:40,
cfranchisetype:6,ccanadianflag:1,coldfranchisecode:1,ctransactiondate:4,crlf:2'
;
*/

DROP TABLE IF EXISTS DDValues3_franchise;--2.532 sec

SELECT DISTINCT (LTRIM(RTRIM(UPPER(cSICCode))) + LTRIM(RTRIM(UPPER(cConvertedFranchise)))  ) as cValue, LTRIM(RTRIM(cFranchiseName)) as cDescription    
INTO DDvalues3_franchise 
from sql_tblSICFranchiseCode
WHERE
(cValue) <> '' or (cDescription) <>  '';


drop table if exists DDvalues3_franchise_clean;--1min 7 sec
CREATE table DDvalues3_franchise_clean
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT 
      a.id as id,
       DD_F1.cDescription AS  FRANCHISE_CODE_1_DESCRIPTION,
       DD_F2.cDescription AS FRANCHISE_CODE_2_DESCRIPTION,
       DD_F3.cDescription AS FRANCHISE_CODE_3_DESCRIPTION, 
       DD_F4.cDescription AS FRANCHISE_CODE_4_DESCRIPTION,
       DD_F5.cDescription AS FRANCHISE_CODE_5_DESCRIPTION,
       DD_F6.cDescription AS FRANCHISE_CODE_6_DESCRIPTION
FROM tblBusinessIndividual a
left join DDValues3_franchise DD_F1  ON A.PRIMARYSIC6 + A.FRANCHISE_CODE_1 = DD_F1.cValue and  A.FRANCHISE_CODE_1 <>''
left join DDValues3_franchise DD_F2  ON A.PRIMARYSIC6 + A.FRANCHISE_CODE_2 = DD_F2.cValue and  A.FRANCHISE_CODE_2 <>''
left join DDValues3_franchise DD_F3  ON A.PRIMARYSIC6 + A.FRANCHISE_CODE_3 = DD_F3.cValue and  A.FRANCHISE_CODE_3 <>''
left join DDValues3_franchise DD_F4  ON A.PRIMARYSIC6 + A.FRANCHISE_CODE_4 = DD_F4.cValue and  A.FRANCHISE_CODE_4 <>''
left join DDValues3_franchise DD_F5  ON A.PRIMARYSIC6 + A.FRANCHISE_CODE_5 = DD_F5.cValue and  A.FRANCHISE_CODE_5 <>''
left join DDValues3_franchise DD_F6  ON A.PRIMARYSIC6 + A.FRANCHISE_CODE_6 = DD_F6.cValue and  A.FRANCHISE_CODE_6 <>'';

DROP TABLE IF EXISTS DDvalues3_franchise;
CREATE TABLE DDvalues3_franchise 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT
id,
min(FRANCHISE_CODE_1_DESCRIPTION) as  FRANCHISE_CODE_1_DESCRIPTION,
min(FRANCHISE_CODE_2_DESCRIPTION) as FRANCHISE_CODE_2_DESCRIPTION,
min(FRANCHISE_CODE_3_DESCRIPTION) as FRANCHISE_CODE_3_DESCRIPTION,
min(FRANCHISE_CODE_4_DESCRIPTION) as FRANCHISE_CODE_4_DESCRIPTION,
min(FRANCHISE_CODE_5_DESCRIPTION) as FRANCHISE_CODE_5_DESCRIPTION,
min(FRANCHISE_CODE_6_DESCRIPTION) as FRANCHISE_CODE_6_DESCRIPTION
from DDvalues3_franchise_clean
GROUP BY ID
;