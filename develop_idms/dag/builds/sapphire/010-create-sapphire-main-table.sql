DROP TABLE IF EXISTS {sapphire-tbl-main};
CREATE TABLE IF NOT EXISTS {sapphire-tbl-main}(
id integer ENCODE delta IDENTITY(1, 1),
cinclude character(1) ENCODE zstd,
epd_haspostal character(1) ENCODE zstd,
epd_hasphone character(1) ENCODE zstd,
listid integer ENCODE az64,
state varchar(2) ENCODE zstd,
addresstype character(1) ENCODE zstd,
gender character(1) ENCODE zstd,
noofaddresslines character(1) ENCODE zstd,
phonenumbertype character(1) ENCODE zstd,
prefixcode character(1) ENCODE zstd,
listtype character(1) ENCODE zstd,
productcode varchar(2) ENCODE zstd,
text_jobfunction1 varchar(5) ENCODE zstd,
text_jobtitle varchar(5) ENCODE zstd,
recordtype character(1) ENCODE zstd,
addresstypeindicator character(1) ENCODE zstd,
deliverypointdropind character(1) ENCODE zstd,
deliverycode character(1) ENCODE zstd,
deliverytype character(1) ENCODE zstd,
dmamailpreference character(1) ENCODE zstd,
dmaphonesuppress character(1) ENCODE zstd,
lot varchar(2) ENCODE zstd,
mailabilityscore varchar(2) ENCODE zstd,
resbusindicator character(1) ENCODE zstd,
movetype character(1) ENCODE zstd,
seasonalindicator character(1) ENCODE zstd,
vacantindicator character(1) ENCODE zstd,
prisonrecord character(1) ENCODE zstd,
permissiontype character(1) ENCODE zstd,
lotinfo varchar(5) ENCODE zstd,
sic4 varchar(150) ENCODE zstd,
sic2 varchar(150) ENCODE zstd,
canadiansic4 varchar(150) ENCODE zstd,
canadiansic2 varchar(150) ENCODE zstd,
activeexpire character(50) ENCODE zstd,
epd_domaintype character(1) ENCODE zstd,
epd_scf varchar(3) ENCODE zstd,
epd_location character(1) ENCODE zstd,
deliverypoint varchar(12) ENCODE zstd,
batchdate varchar(150) ENCODE zstd,
carrierroute varchar(4) ENCODE zstd,
individual_id varchar(17) ENCODE zstd,
company_mc varchar(15) ENCODE zstd,
accountno varchar(15) ENCODE zstd,
fullname varchar(150) ENCODE zstd,
firstname varchar(120) ENCODE zstd,
lastname varchar(120) ENCODE zstd,
title varchar(200) ENCODE zstd,
company varchar(150) ENCODE zstd,
addressline1 varchar(150) ENCODE zstd,
addressline2 varchar(150) ENCODE zstd,
city varchar(130) ENCODE zstd,
zipfull varchar(100) ENCODE zstd,
zip varchar(5) ENCODE zstd,
zip4 varchar(4) ENCODE zstd,
websiteurl varchar(500) ENCODE zstd,
loginsiteurlip varchar(50) ENCODE zstd,
emailaddress_md5 varchar(32) ENCODE zstd,
encryptedemail varchar(120) ENCODE zstd,
epd_domain varchar(400) ENCODE zstd,
emailaddress varchar(400) ENCODE zstd,
countryname varchar(200) ENCODE zstd,
phone varchar(20) ENCODE zstd,
sha256lower varchar(64) ENCODE zstd,
sqltableid bigint ENCODE delta
)
DISTSTYLE key distkey ( individual_id )
sortkey (individual_id);

COPY
{sapphire-tbl-main}
FROM 's3://{s3-internal}{s3-key-tblmain}{build_id}'
--FROM 's3://{s3-internal}{s3-key-tblmain}'
--FROM 's3://idms-2722-internalfiles/Sapphire/IQLoadFiles/DW_Final_71_{build_id}_14547'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

DROP TABLE IF EXISTS {sapphire-pre-build};
CREATE TABLE {sapphire-pre-build} (
    individual_id varchar(22) ENCODE zstd,
    newnamedate varchar(11) ENCODE zstd,
    emailaddress varchar(405) ENCODE zstd,
    phone varchar(25) ENCODE zstd,
    phone_quality_flag character(6) ENCODE zstd,
    email_suppression_flag character(6) ENCODE zstd,
    dm_tm_suppression_flag character(6) ENCODE zstd,
    lastclickopen varchar(11) ENCODE zstd,
    emailfrequency_90 integer ENCODE az64,
    parksuppression character(6) ENCODE zstd,
    briteverifiedemail varchar(8) ENCODE zstd,
    emailscoreflag varchar(6) ENCODE zstd,
    cinclude character(6) ENCODE zstd,
    text_jobtitle varchar(10) ENCODE zstd,
    employeescombined_prioritized varchar(15) ENCODE zstd,
    salesforcompany_prioritized varchar(15) ENCODE zstd
)
DISTSTYLE key distkey ( individual_id )
sortkey (individual_id);

INSERT INTO {sapphire-pre-build}
SELECT individual_id,
    newnamedate,
    emailaddress,
    phone,
    phone_quality_flag,
    email_suppression_flag,
    dm_tm_suppression_flag,
    lastclickopen,
    emailfrequency_90,
    parksuppression,
    briteverifiedemail,
    emailscoreflag,
    cinclude,
    text_jobtitle,
    employeescombined_prioritized,
    salesforcompany_prioritized
    from tblMain_{pre_buildid}_{pre_build};
--from abhishek_sapphire_tobedropped;