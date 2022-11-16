DROP TABLE IF EXISTS {sapphire-tbl-ctas1};
CREATE TABLE {sapphire-tbl-ctas1} as
select id,
       CASE
           WHEN cinclude = '' AND emailaddress <> '' THEN 'Y'
           WHEN cinclude = '' THEN 'X'
           WHEN cinclude = 'M' THEN 'Y'
           ELSE cinclude END                                                                       as cinclude,
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
       CASE WHEN text_jobfunction1 = '' THEN 'Z9Z' ELSE text_jobfunction1 END                      AS text_jobfunction,
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
       CASE WHEN fullname LIKE '%"%' THEN REPLACE(fullname, '"', '') ELSE fullname END             as fullname,
       CASE
           WHEN firstname LIKE '%"%' THEN REPLACE(firstname, '"', '')
           ELSE firstname END                                                                      AS firstname_transformed,
       CASE WHEN lastname LIKE '%"%' THEN REPLACE(lastname, '"', '') ELSE lastname END             AS lastname,
       title                                                                                       as title_raw,
       CASE
           WHEN Title <> '' and Title is not NULL AND Text_JobTitle = '' AND (Text_JobFunction = 'Z9Z') THEN ''
           WHEN Title <> '' and Title is not NULL
               AND Title <> REPLACE(REPLACE(REPLACE(Title, '"', ''), '*', ''), '?', '')
               THEN REPLACE(REPLACE(REPLACE(Title, '"', ''), '*', ''), '?', '')
           WHEN Title <> '' and Title is not NULL
               AND Title <> REPLACE(
                       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Title, '?', ''), '?', ''), '®', ''), '?', ''), '', ''),
                       '?', '')
               THEN REPLACE(
                   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Title, '?', ''), '?', ''), '®', ''), '?', ''), '', ''), '?',
                   '')
           WHEN Title <> ''
               AND Left(ltrim(rtrim(Title)), 1) IN
                   ('~', '!', '@', '$', '%', '^', '&', '*', ')', '{', '}', '"', '?', '/', ':', ';', '+', '<', '>', '.',
                    '-', '=', '\\', '?', '?', '®', '?', '')
               THEN SUBSTRING(ltrim(rtrim(Title)), 2, LEN(ltrim(rtrim(Title))))
           WHEN Title like '%\x0a%' Or Title like '%\x0d%'
               THEN Replace(Replace(Title, '\x0a', ''), '\x0d', '')
           ELSE title END                                                                          AS title_transormed,
       CASE
           WHEN company LIKE '%"%' THEN REPLACE(Company, '"', '')
           when company like '%\x0a%' Or company like '%\x0d%' THEN Replace(Replace(company, '\x0a', ''), '\x0d', '')
           else company END                                                                        AS company,
       CASE WHEN addressline1 LIKE '%"%' THEN REPLACE(Addressline1, '"', '') else addressline1 END AS addressline1,
       CASE WHEN addressline2 LIKE '%"%' THEN REPLACE(Addressline2, '"', '') else addressline2 END AS addressline2,
       CASE WHEN city LIKE '%"%' THEN REPLACE(city, '"', '') else city END                         AS city,
       zipfull,
       zip,
       zip4,
       websiteurl,
       loginsiteurlip,
       emailaddress_md5,
       encryptedemail,
       epd_domain,
       emailaddress,
       CASE
           WHEN countryname = '' and trim(upper(state)) IN
                                     ('AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'ID', 'IL', 'IN',
                                      'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN',
                                      'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK',
                                      'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI',
                                      'WY', 'AK', 'HI', 'PR', 'QU', 'VI', 'AA', 'AE', 'AP', 'FP') THEN 'USA'
           WHEN countryname = '' and trim(upper(state)) IN
                                     ('AB', 'BC', 'MB', 'NF', 'NS', 'NT', 'ON', 'PE', 'PQ', 'SK', 'YT', 'NB', 'NL',
                                      'QC', 'NU') THEN 'CANADA'
           WHEN countryname = 'UNITED STATES' THEN 'USA'
           ELSE countryname END                                                                    as countryname,
       CASE
           WHEN Phone like '%\x0a%' Or Phone like '%\x0d%' then Replace(Replace(Phone, '\x0a', ''), '\x0d', '')
           else phone end                                                                          as phone,
       sha256lower,
       sqltableid,
       CASE WHEN FirstName <> '' AND LastName <> '' THEN 'Y' ELSE 'N' END                          AS hascontact,
       CASE WHEN Zip4 <> '' THEN 'Y' ELSE 'N' END                                                  as haszip4,
       CASE WHEN WebsiteURL <> '' THEN 'Y' ELSE 'N' END                                            AS hasurl,
       0                                                                                           AS EmailFrequency_90,
       CASE
           WHEN Individual_ID IN
                (SELECT Individual_ID
                 FROM {sapphire-tbl-main}
                 WHERE ListID IN (6124, 6063, 6061, 6062)) THEN 'Y'
           ELSE 'N' END                                                 AS InfoUSAFlag,
       CASE
           WHEN countryname = 'USA' AND state in
                                        ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA',
                                         'ID', 'IL', 'IN', 'KS',
                                         'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE',
                                         'NH', 'NJ', 'NM', 'NV',
                                         'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT',
                                         'WA', 'WI', 'WV', 'WY') THEN 'A'
           WHEN COUNTRYNAME = 'USA'
               AND STATE in ('AA', 'AE', 'AP', 'AS', 'FM', 'GU', 'MH', 'MP', 'PR', 'PW', 'VI') THEN 'B'
           WHEN COUNTRYNAME = 'CANADA'
               AND STATE in ('AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT') THEN 'C'
           ELSE 'F' END                                                                            as countryflag,
       CASE WHEN countryname = 'CANADA' THEN LEFT(Zipfull, 3) ELSE '' END                          AS SCF_CANADA,
       CASE
           WHEN Text_JobTitle is null or ltrim(rtrim(Text_JobTitle)) = '' THEN 'NULL'
           ELSE LTRIM(RTRIM(Text_JobTitle)) END + '-'
           + CASE
                 When Text_JobFunction is null or ltrim(rtrim(Text_JobFunction)) = '' THEN 'NULL'
                 ELSE LTRIM(RTRIM(Text_JobFunction)) END                                           AS CombinedJobTitleAndFunction,
       CASE
           WHEN LTRIM(RTRIM(title_transormed)) <> ''
               AND LEN(firstname_transformed) > 2 and (NOT substring(firstname_transformed, 2, 1) in (' ', '.', ':')) THEN 'Y'
           ELSE 'N' END                                                                            as HasNameAndTitle,
       'N' as Phone_CellFlag,
       '' as Phone_quality_flag,
       CASE WHEN EPD_domain <>'' THEN RIGHT(EPD_domain, LEN(EPD_domain) - CHARINDEX('.', EPD_domain) +1) END as TopLevelDomain,
       left (company_mc,11) as MaxPerAddressZip,
       CASE WHEN Individual_ID in (Select Individual_ID from {sapphire-pre-build} where Email_Suppression_Flag = 'Y') THEN 'Y'
            ELSE 'N' END AS Email_Suppression_Flag,
       CASE WHEN Individual_ID in (Select  Individual_ID from  {sapphire-pre-build}  where DM_TM_Suppression_Flag  = 'Y') THEN 'Y'
            ELSE 'N' END AS DM_TM_Suppression_Flag
FROM {sapphire-tbl-main};

ALTER TABLE {sapphire-tbl-ctas1}
    RENAME COLUMN Text_JobFunction TO Text_JobFunction1;
ALTER TABLE {sapphire-tbl-ctas1}
    RENAME COLUMN title_transormed TO title;
ALTER TABLE {sapphire-tbl-ctas1}
    RENAME COLUMN firstname_transformed TO firstname;

DROP TABLE IF EXISTS #DropCanadaEmails;
CREATE TABLE #DropCanadaEmails
(
    ID int ENCODE delta
);
INSERT into #DropCanadaEmails (ID)
SELECT ID
FROM {sapphire-tbl-ctas1}
where cinclude in ('Y', 'M')
  and EmailAddress is not null
  and EmailAddress <> ''
  and (
        (EmailAddress LIKE '%.CA' OR (EPD_Domain LIKE '%.CA' OR COUNTRYNAME = 'CANADA'))
        OR (COUNTRYNAME <> 'CANADA' and ZipFull <> '' AND REGEXP_COUNT(LEFT(ZipFull, 5), '^[0-9]+$') = 0)
    );

ALTER TABLE {sapphire-tbl-ctas1}
    ADD canada_raw01 varchar(80) ENCODE zstd;
UPDATE {sapphire-tbl-ctas1}
SET canada_raw01    = LEFT(A.EmailAddress, 80),
    EmailAddress='',
    EmailAddress_MD5='',
    EncryptedEmail='',
    cInclude='C'
FROM {sapphire-tbl-ctas1} A
         INNER JOIN #DropCanadaEmails B ON A.id = B.ID;

DROP TABLE IF EXISTS #DropCanadaEmails;

DROP TABLE IF EXISTS #MultiBuyerCount;
CREATE TABLE #MultiBuyerCount
(
    Individual_ID varchar(17) ENCODE ZSTD,
    ListCount     int ENCODE ZSTD,
    distListCount int ENCODE ZSTD
)
DISTSTYLE key distkey ( individual_id )
sortkey (individual_id);

INSERT INTO #MultiBuyerCount (Individual_ID, ListCount, distListCount)
Select Individual_ID, count(ListID) as ListCount, count(Distinct ListID) as distListCount
from {sapphire-tbl-ctas1}
group by Individual_ID;

ALTER TABLE {sapphire-tbl-ctas1}
    ADD MultiBuyerCount varchar(80) ENCODE zstd;
UPDATE {sapphire-tbl-ctas1}
SET MultiBuyerCount = b.distListCount
FROM {sapphire-tbl-ctas1} A
         inner join #MultiBuyerCount B
                    on A.Individual_ID = B.Individual_ID;

Drop table IF EXISTS #MultiBuyerCount;

--InfoUSA Flag SET IN CTAS

ALTER TABLE {sapphire-tbl-ctas1}
    ADD MultiBuyerCount_ByEmail INT;
DROP TABLE IF EXISTS #MultiBuyerCount;
CREATE TABLE #MultiBuyerCount
(
    EmailAddress  varchar(65),
    ListCount     int,
    distListCount int
);
INSERT INTO #MultiBuyerCount (EmailAddress, ListCount, distListCount)
Select EmailAddress, count(ListID) as ListCount, count(Distinct ListID) as distListCount
from {sapphire-tbl-ctas1}
group by EmailAddress;

UPDATE {sapphire-tbl-ctas1}
SET MultiBuyerCount_ByEmail = b.distListCount
From {sapphire-tbl-ctas1} A
         inner join #MultiBuyerCount B
                    on A.EmailAddress = B.EmailAddress
where A.EmailAddress <> '';

Drop table IF EXISTS #MultiBuyerCount;

/*-- Append Infousa Fields   (InfoGroup - US Business File)
--Add GUS_YRESTBLSHD  from tblExternal13_191_201206  */

ALTER TABLE {sapphire-tbl-ctas1}
    add INFO_YRESTBLSHD char(4);
ALTER TABLE {sapphire-tbl-ctas1}
    add IGUS_ABINUM VARCHAR(9);
UPDATE {sapphire-tbl-ctas1}
SET INFO_YRESTBLSHD = b.IGUS_YRESTBLSHD,
    IGUS_ABINUM     = b.igus_abinum
FROM {sapphire-tbl-ctas1} A
         INNER JOIN tblExternal13_191_201206 b
                    ON A.Company_MC = b.Company_MC;

ALTER TABLE {sapphire-tbl-ctas1}
    add IGCND_LOCNUM VARCHAR(9);
UPDATE {sapphire-tbl-ctas1}
SET IGCND_LOCNUM = b.IGCND_LOCNUM
FROM {sapphire-tbl-ctas1} A
         INNER JOIN tblExternal14_191_201206 b
                    ON A.Company_MC = b.Company_MC;


ALTER TABLE {sapphire-tbl-ctas1}
    add CountByCompanyMC int;
Drop table IF EXISTS #CountByCompanyMC;
CREATE TABLE #CountByCompanyMC
(
    Company_MC varchar(15),
    distCount  int
);
/*--Get the count by  Company_MC*/
INSERT INTO #CountByCompanyMC (Company_MC, distCount)
Select Company_MC, count(Distinct Individual_ID) as disCount
from {sapphire-tbl-ctas1}
group by Company_MC;

Update {sapphire-tbl-ctas1}
SET CountByCompanyMC = b.distCount
From {sapphire-tbl-ctas1} A
         inner join #CountByCompanyMC B
                    on A.Company_MC = B.Company_MC
where A.Company_MC <> '';

Drop table IF EXISTS #CountByCompanyMC;
ALTER TABLE {sapphire-tbl-ctas1}
    add LastClickOpen varchar(6);
ALTER TABLE {sapphire-tbl-ctas1}
    add Parksuppression char(1);

/*--Now,  import from previous  build*/
-- UPDATE {sapphire-tbl-ctas1}
-- SET LastClickOpen     = B.LastClickOpen,
--     EmailFrequency_90 = B.EmailFrequency_90,
--     Parksuppression   = B.Parksuppression
-- FROM {sapphire-tbl-ctas1} A
--          INNER JOIN tblMain_{TASK(1|Result.Parameter.Value|pre_buildid)} _{TASK(1|Result.Parameter.Value|pre_build)} B
-- on A.EmailAddress = B.EmailAddress
-- where A.EmailAddress <>'';

ALTER TABLE {sapphire-tbl-ctas1}
    ADD BriteVerifiedEmail VARCHAR(3) ENCODE ZSTD;
ALTER TABLE {sapphire-tbl-ctas1}
    ADD EmailScoreFlag VARCHAR(1) ENCODE ZSTD;

-- UPDATE {sapphire-tbl-ctas1}
-- SET BriteVerifiedEmail = B.BriteVerifiedEmail,
--     EmailScoreFlag     = B.EmailScoreFlag,
--     cInclude = CASE WHEN A.cInclude ='Y' and b.cinclude <>'Y' then B.cInclude else A.cinclude end
-- FROM {sapphire-tbl-ctas1} A
--          INNER JOIN tblMain_{TASK(1|Result.Parameter.Value|pre_buildid)} _{TASK(1|Result.Parameter.Value|pre_build)} B
-- on A.EmailAddress = B.EmailAddress
-- where A.EmailAddress <>'' and B.EmailAddress <>'';

--combined two updates
UPDATE {sapphire-tbl-ctas1}
SET LastClickOpen     = B.LastClickOpen,
    EmailFrequency_90 = B.EmailFrequency_90,
    Parksuppression   = B.Parksuppression,
    BriteVerifiedEmail = CASE WHEN B.EmailAddress <>'' THEN B.BriteVerifiedEmail ELSE A.BriteVerifiedEmail END,
    EmailScoreFlag = CASE WHEN B.EmailAddress <>'' THEN B.EmailScoreFlag ELSE A.EmailScoreFlag END,
    cInclude = CASE WHEN B.EmailAddress <>'' AND A.cInclude ='Y' and b.cinclude <>'Y' then B.cInclude else A.cinclude end
FROM {sapphire-tbl-ctas1} A
         INNER JOIN {sapphire-pre-build} B
on A.EmailAddress = B.EmailAddress
where A.EmailAddress <>'';

/*--Now,  import the cInclude from previous  build*/
-- UPDATE {sapphire-tbl-ctas1}
-- SET cInclude = B.cInclude
-- FROM {sapphire-tbl-ctas1} A
--          INNER JOIN tblMain_{TASK(1|Result.Parameter.Value|pre_buildid)} _{TASK(1|Result.Parameter.Value|pre_build)} B
-- on A.EmailAddress = B.EmailAddress
-- where A.EmailAddress <>''
--   and B.EmailAddress <>''
--   and A.cInclude ='Y'
--   and b.cinclude <>'Y';

Update {sapphire-tbl-ctas1}
SET cInclude ='E'
where EmailAddress = ''
  and cInclude <> 'E';

drop table IF EXISTS #CityState;
CREATE TABLE IF NOT EXISTS #CityState
(
    ZipCode      VarChar(10),
    CBSA         Varchar(10),
    CITY_STATE   Varchar(50),
    COUNTY_STATE Varchar(50)
);

COPY #CityState
    from 's3://{s3-internal}{s3-key-buildsupportfiles}/Zip_City_County_Table_Pipe.txt'
    iam_role '{iam}'
    ACCEPTINVCHARS FILLRECORD
    delimiter '|'
    ignoreheader as 1;

UPDATE #CityState
SET ZipCode      = UPPER(LTRIM(RTRIM(ZipCode))),
    CBSA         = UPPER(LTRIM(RTRIM(CBSA))),
    CITY_STATE   = UPPER(LTRIM(RTRIM(CITY_STATE))),
    COUNTY_STATE =UPPER(LTRIM(RTRIM(COUNTY_STATE)));

DELETE
from #CityState
where UPPER(ZipCode) = 'ZIPCODE'
   or ZipCode = '';

UPDATE #CityState
SET CITY_STATE   = REPLACE(CITY_STATE, ' ', ''),
    COUNTY_STATE =REPLACE(COUNTY_STATE, ' ', '');

ALTER table {sapphire-tbl-ctas1}
    ADD CBSA VARCHAR(10);
ALTER table {sapphire-tbl-ctas1}
    ADD CITY_STATE VARCHAR(50);
ALTER table {sapphire-tbl-ctas1}
    ADD COUNTY_STATE VARCHAR(50);

Update {sapphire-tbl-ctas1}
SET CBSA         = b.CBSA,
    CITY_STATE   = b.CITY_STATE,
    COUNTY_STATE =b.COUNTY_STATE
From {sapphire-tbl-ctas1} A
         inner join #CityState B
                    on A.ZIP = B.ZipCode
where A.ZIP <> '';
Drop table IF EXISTS #CityState;
