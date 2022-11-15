DROP TABLE IF EXISTS nosuchtable;

-- These tables are created on IDMS Main Cluster. We need to create them for ETL cluster
DROP TABLE IF EXISTS EXCLUDE_DONOTCALLFLAG;

CREATE TABLE EXCLUDE_DONOTCALLFLAG (cPhone varchar(10) primary key distkey sortkey);

COPY EXCLUDE_DONOTCALLFLAG
FROM 's3://{s3-axle-gold}/do-not-call/donotcall'
IAM_ROLE '{iam}'
FORMAT PARQUET;

DROP TABLE IF EXISTS tbldqi_cellphone;

CREATE TABLE tbldqi_cellphone (
    cellphone_areacode character varying(3) ENCODE zstd,
    cellphone_number character varying(7) ENCODE zstd,
    firstname character varying(50) ENCODE zstd,
    middleinitial character varying(10) ENCODE zstd,
    lastname character varying(50) ENCODE zstd,
    gender character varying(10) ENCODE zstd,
    housenumber character varying(20) ENCODE zstd,
    streetname character varying(38) ENCODE zstd,
    streetpostdirectional character varying(21) ENCODE zstd,
    streetpredirectional character varying(21) ENCODE zstd,
    streetsuffix character varying(41) ENCODE zstd,
    unitnumber character varying(81) ENCODE zstd,
    unittype character varying(41) ENCODE zstd,
    city character varying(28) ENCODE zstd,
    state character varying(2) ENCODE zstd,
    zipcode character varying(5) ENCODE zstd,
    zipfour character varying(4) ENCODE zstd,
    zipfourtype character varying(11) ENCODE zstd,
    cbsacode character varying(41) ENCODE zstd,
    latitude character varying(11) ENCODE zstd,
    longitude character varying(11) ENCODE zstd,
    fipscountycode character varying(3) ENCODE zstd,
    fipsstatecode character varying(2) ENCODE zstd,
    cordcutter character varying(1) ENCODE zstd,
    prepaid_indicator character varying(1) ENCODE zstd,
    verified_code character varying(1) ENCODE zstd,
    activitystatus character varying(2) ENCODE zstd,
    filterflag character varying(1) ENCODE zstd,
    filterreason character varying(2) ENCODE zstd,
    isdqi character varying(1) ENCODE zstd,
    matchlevel character varying(7) ENCODE zstd,
    individualmatch character varying(1) ENCODE zstd,
    matchscore character varying(3) ENCODE zstd,
    modifiedscore character varying(3) ENCODE zstd,
    lemsmatchcode character varying(18) ENCODE zstd,
    company_id bigint ENCODE az64,
    individual_id bigint ENCODE az64,
    donotcallflag character varying(1) ENCODE zstd,
    id integer ENCODE delta,
    cellphone character varying(10) ENCODE zstd primary key sortkey
)
DISTSTYLE ALL;

COPY tblDQI_CellPhone
FROM 's3://{s3-axle-gold}/wireless-parquet/'
IAM_ROLE '{iam}'
FORMAT PARQUET;


--------------------------
DROP TABLE IF EXISTS sql_tblState;

CREATE TABLE sql_tblState
(
    cstatecode varchar(2) null,
	cstate varchar(50) null,
	ccountycode varchar(3) null,
	ccounty  varchar(50) null,
	ccity  varchar(50) null,
	czip varchar(5) null,
	databaseid int default 0)
DISTSTYLE ALL
COMPOUND SORTKEY (cstatecode,czip);

COPY sql_tblState
FROM 's3://{s3-internal}/FilesExportedFromIQ/etl_lookupdata/sql_tblState.txt'
IAM_ROLE '{iam}';

-------------------------------------------
DROP TABLE IF EXISTS sql_tblDDDescriptions;

CREATE TABLE sql_tblDDDescriptions
(
	id integer not null,
	DatabaseID int not null ,
	cFieldName varchar(100) null,
	cvalue varchar(200) null,
	cdescription varchar(1000) not null,
	dcreateddate varchar(25) not null ,
	ccreatedby varchar(25) not null,
	dmodifieddate varchar(25) null,
	cmodifiedby varchar(25) null)
DISTSTYLE ALL
COMPOUND SORTKEY(DatabaseID,cFieldName);

COPY sql_tblDDDescriptions
FROM 's3://{s3-internal}/FilesExportedFromIQ/etl_lookupdata/sql_tblDDDescriptions.txt'
IAM_ROLE '{iam}'
ACCEPTINVCHARS;

