DROP TABLE IF EXISTS {sapphire-main-tbl-id};
CREATE TABLE {sapphire-main-tbl-id} (
	id integer ENCODE az64 distkey ,
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
    title_raw character varying(200) ENCODE zstd,
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
	sqltableid bigint ENCODE delta,
    hascontact character varying(1) ENCODE zstd,
    haszip4 character varying(1) ENCODE zstd,
    hasurl character varying(1) ENCODE zstd,
    emailfrequency_90 integer ENCODE az64,
    infousaflag character varying(1) ENCODE zstd,
    countryflag character varying(1) ENCODE zstd,
    scf_canada character varying(100) ENCODE zstd,
    combinedjobtitleandfunction character varying(11) ENCODE zstd,
    hasnameandtitle character varying(1) ENCODE zstd,
    phone_cellflag character varying(1) ENCODE zstd,
    phone_quality_flag character varying(1) ENCODE zstd,
    topleveldomain character varying(400) ENCODE zstd,
    maxperaddresszip character varying(15) ENCODE zstd,
    email_suppression_flag character varying(1) ENCODE zstd,
    dm_tm_suppression_flag character varying(1) ENCODE zstd,
    canada_raw01 character varying(80) ENCODE lzo,
    multibuyercount character varying(80) ENCODE zstd,
    multibuyercount_byemail integer ENCODE zstd,
    info_yrestblshd character(4) ENCODE zstd,
    igus_abinum character varying(9) ENCODE zstd,
    igcnd_locnum character varying(9) ENCODE zstd,
    countbycompanymc integer ENCODE az64,
    lastclickopen character varying(6) ENCODE zstd,
    parksuppression character(1) ENCODE zstd,
    briteverifiedemail character varying(3) ENCODE zstd,
    emailscoreflag character varying(1) ENCODE zstd,
    cbsa character varying(10) ENCODE zstd,
    city_state character varying(50) ENCODE zstd,
    county_state character varying(50) ENCODE zstd,
    newnamedate character varying(6) ENCODE bytedict,
    newemaildate character varying(6) ENCODE bytedict,
    liverampmatchback_email character(1) ENCODE lzo,
    liverampmatchback_postal character(1) ENCODE lzo,
    phonemultibuyercount integer ENCODE zstd,
    template_flag character varying(70)  ENCODE zstd,
    removeddata_raw02 character varying(80) ENCODE zstd
)
DISTSTYLE KEY
SORTKEY ( id );

INSERT INTO {sapphire-main-tbl-id}
SELECT id,
cinclude,
epd_haspostal,
epd_hasphone,
listid,
state,
addresstype,
gender,
noofaddresslines,
phonenumbertype,
prefixcode,
listtype,
productcode,
text_jobfunction1,
text_jobtitle,
recordtype,
addresstypeindicator,
deliverypointdropind,
deliverycode,
deliverytype,
dmamailpreference,
dmaphonesuppress,
lot,
mailabilityscore,
resbusindicator,
movetype,
seasonalindicator,
vacantindicator,
prisonrecord,
permissiontype,
lotinfo,
sic4,
sic2,
canadiansic4,
canadiansic2,
activeexpire,
epd_domaintype,
epd_scf,
epd_location,
deliverypoint,
batchdate,
carrierroute,
individual_id,
company_mc,
accountno,
fullname,
firstname,
lastname,
title_raw,
title,
company,
addressline1,
addressline2,
city,
zipfull,
zip,
zip4,
websiteurl,
loginsiteurlip,
emailaddress_md5,
encryptedemail,
epd_domain,
emailaddress,
countryname,
phone,
sha256lower,
sqltableid,
hascontact,
haszip4,
hasurl,
emailfrequency_90,
infousaflag,
countryflag,
scf_canada,
combinedjobtitleandfunction,
hasnameandtitle,
phone_cellflag,
phone_quality_flag,
topleveldomain,
maxperaddresszip,
email_suppression_flag,
dm_tm_suppression_flag,
canada_raw01,
multibuyercount,
multibuyercount_byemail,
info_yrestblshd,
igus_abinum,
igcnd_locnum,
countbycompanymc,
lastclickopen,
parksuppression,
briteverifiedemail,
emailscoreflag,
cbsa,
city_state,
county_state,
newnamedate,
newemaildate,
liverampmatchback_email,
liverampmatchback_postal,
phonemultibuyercount,
template_flag,
removeddata_raw02
FROM {sapphire-tbl-ctas1} ;

DROP TABLE IF EXISTS {sapphire-ctas-tbl-id};
CREATE TABLE {sapphire-ctas-tbl-id} (
    id integer ENCODE delta distkey,
    individual_id character varying(17) ENCODE zstd ,
    company_mc character varying(15) ENCODE zstd,
    text_jobtitle character varying(5) ENCODE zstd,
    titlegotpromoted character varying(1) ENCODE zstd,
    employeegrowth character varying(1) ENCODE zstd,
    salesgrowth character varying(1) ENCODE zstd,
    salesforcompany_prioritized character varying(10) ENCODE zstd,
    employeescombined_prioritized character varying(10) ENCODE zstd,
    industry_prioritized character varying(6) ENCODE zstd,
    sic4_prioritized character varying(150) ENCODE zstd,
    sic2_prioritized character varying(2) ENCODE zstd,
    employeesize_prioritized character varying(10) ENCODE zstd,
    employeesatcompany_prioritized character varying(10) ENCODE zstd,
    salesvolume_prioritized character varying(10) ENCODE zstd,
    salescombined_prioritized character varying(5) ENCODE zstd,
    text_jobfunction_rollup character varying(2) ENCODE zstd,
    text_jobtitle_rollup character varying(2) ENCODE zstd,
    combinedjobtitleandfunction_rollup character varying(6) ENCODE zstd,
    text_jobfunction1 character varying(5) ENCODE zstd
)
DISTSTYLE KEY
SORTKEY ( id );

INSERT INTO {sapphire-ctas-tbl-id}
SELECT id,
       individual_id,
       Company_MC,
       Text_Jobtitle,
       TitleGotPromoted,
       EmployeeGrowth,
       SalesGrowth,
       SalesForCompany_Prioritized,
       EmployeesCombined_Prioritized,
       Industry_Prioritized,
       SIC4_Prioritized,
       SIC2_Prioritized,
       EmployeeSize_Prioritized,
       EmployeesAtCompany_Prioritized,
       SalesVolume_Prioritized,
       SalesCombined_Prioritized,
       Text_JobFunction_Rollup,
       Text_JobTitle_Rollup,
       CombinedJobTitleAndFunction_Rollup,
       Text_JobFunction1
FROM {sapphire-update1-ctas};

DROP TABLE IF EXISTS {sapphire-main-ctas};
CREATE TABLE {sapphire-main-ctas} AS
    SELECT
main.id,
main.cinclude,
main.epd_haspostal,
main.epd_hasphone,
main.listid,
main.state,
main.addresstype,
main.gender,
main.noofaddresslines,
main.phonenumbertype,
main.prefixcode,
main.listtype,
main.productcode,
main.text_jobfunction1,
main.text_jobtitle,
main.recordtype,
main.addresstypeindicator,
main.deliverypointdropind,
main.deliverycode,
main.deliverytype,
main.dmamailpreference,
main.dmaphonesuppress,
main.lot,
main.mailabilityscore,
main.resbusindicator,
main.movetype,
main.seasonalindicator,
main.vacantindicator,
main.prisonrecord,
main.permissiontype,
main.lotinfo,
main.sic4,
main.sic2,
main.canadiansic4,
main.canadiansic2,
main.activeexpire,
main.epd_domaintype,
main.epd_scf,
main.epd_location,
main.deliverypoint,
main.batchdate,
main.carrierroute,
main.individual_id,
main.company_mc,
main.accountno,
main.fullname,
main.firstname,
main.lastname,
main.title_raw,
main.title,
main.company,
main.addressline1,
main.addressline2,
main.city,
main.zipfull,
main.zip,
main.zip4,
main.websiteurl,
main.loginsiteurlip,
main.emailaddress_md5,
main.encryptedemail,
main.epd_domain,
main.emailaddress,
main.countryname,
main.phone,
main.sha256lower,
main.sqltableid,
main.hascontact,
main.haszip4,
main.hasurl,
main.emailfrequency_90,
main.infousaflag,
main.countryflag,
main.scf_canada,
main.combinedjobtitleandfunction,
main.hasnameandtitle,
main.phone_cellflag,
main.phone_quality_flag,
main.topleveldomain,
main.maxperaddresszip,
main.email_suppression_flag,
main.dm_tm_suppression_flag,
main.canada_raw01,
main.multibuyercount,
main.multibuyercount_byemail,
main.info_yrestblshd,
main.igus_abinum,
main.igcnd_locnum,
main.countbycompanymc,
main.lastclickopen,
main.parksuppression,
main.briteverifiedemail,
main.emailscoreflag,
main.cbsa,
main.city_state,
main.county_state,
main.newnamedate,
main.newemaildate,
main.liverampmatchback_email,
main.liverampmatchback_postal,
main.phonemultibuyercount,
main.template_flag,
main.removeddata_raw02,
ctas.sic4_prioritized,
ctas.sic2_prioritized,
ctas.employeesize_prioritized,
ctas.employeesatcompany_prioritized,
ctas.employeescombined_prioritized,
ctas.salesvolume_prioritized,
ctas.salesforcompany_prioritized,
ctas.salescombined_prioritized,
ctas.text_jobfunction_rollup,
ctas.text_jobtitle_rollup,
ctas.combinedjobtitleandfunction_rollup,
ctas.titlegotpromoted,
ctas.salesgrowth,
ctas.employeegrowth,
ctas.industry_prioritized
FROM
   {sapphire-main-tbl-id} main
LEFT JOIN
{sapphire-ctas-tbl-id} ctas
ON
main.id = ctas.id;