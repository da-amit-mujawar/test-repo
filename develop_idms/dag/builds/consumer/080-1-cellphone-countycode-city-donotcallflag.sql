DROP TABLE IF EXISTS nosuchtable;
DROP TABLE IF EXISTS IG_CONSUMER_EXCLUDE_DONOTCALLFLAG;
CREATE TABLE IG_CONSUMER_EXCLUDE_DONOTCALLFLAG
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT
    a.id AS id,
    MIN(f.cphone) AS cphone
FROM {new-load-table} a
LEFT JOIN 
(
    SELECT cphone FROM {EXCLUDE_DONOTCALLFLAG-load-table} 
    GROUP BY cphone
) f 
ON (CASE WHEN LTRIM(RTRIM(a.area_code)) NOT LIKE '0%' THEN LTRIM(RTRIM(a.area_code)) + LTRIM(RTRIM(a.phonenumber)) ELSE '' END) = f.cphone
GROUP BY a.id;

DROP TABLE IF EXISTS IG_CONSUMER_Cellphone;
CREATE TABLE IG_CONSUMER_Cellphone
DISTKEY(id)
SORTKEY(id)
AS
SELECT 
         a.id AS id,
         MIN(a.company_id) company_id, 
         MIN(b.cellphone) AS cellphone,
         MIN(b.cellphone_areacode) AS cellphone_areacode,
         MIN(b.cellphone_number) AS cellphone_number,
         MIN(b.cordcutter) AS cellphone_cordcutter,
         MIN(b.prepaid_indicator) AS cellphone_prepaid_indicator,
         MIN(b.donotcallflag) AS cellphone_donotcallflag,
         MIN(b.verified_code) AS cellphone_verified_code,
         MIN(b.activitystatus) AS cellphone_activitystatus,
         MIN(b.filterflag) AS cellphone_filterflag,
         MIN(b.filterreason) AS cellphone_filterreason,
         MIN(b.isdqi) AS cellphone_isdqi,
         MIN(b.matchlevel) AS cellphone_matchlevel,
         MIN(b.matchscore) AS cellphone_matchscore,
         MIN(b.modifiedscore) AS cellphone_modifiedscore,
         MIN(b.individualmatch) AS cellphone_individualmatch     
  FROM {new-load-table} a 
  LEFT JOIN tblDQI_CellPhone b
    ON CAST(a.Individual_ID AS BIGINT) = b.Individual_ID
 WHERE LENGTH(TRIM(a.Individual_ID)) = 12
 GROUP BY a.id;

--HH Level Match
UPDATE IG_CONSUMER_Cellphone
   SET  CellPhone = b.CellPhone,
        CellPhone_Areacode = b.CellPhone_Areacode,
        CellPhone_Number = b.CellPhone_Number,
        CellPhone_Cordcutter = b.Cordcutter,
        CellPhone_Prepaid_Indicator = b.Prepaid_Indicator,
        CellPhone_Donotcallflag = b.Donotcallflag,
        CellPhone_Verified_Code = b.Verified_Code,
        CellPhone_Activitystatus = b.Activitystatus,
        CellPhone_Filterflag = b.Filterflag,
        CellPhone_Filterreason = b.Filterreason,
        CellPhone_Isdqi = b.Isdqi,
        CellPhone_Matchlevel = b.Matchlevel,
        CellPhone_Matchscore = b.Matchscore,
        CellPhone_Modifiedscore = b.Modifiedscore,
        CellPhone_IndividualMatch = b.IndividualMatch
  FROM IG_CONSUMER_Cellphone a
 INNER JOIN tblDQI_CellPhone b
    ON CAST(a.Company_ID AS BIGINT) = b.Company_ID
 WHERE LENGTH(TRIM(a.Company_ID)) = 12
   AND a.CellPhone IS NULL;


DROP TABLE IF EXISTS IG_CONSUMER_Countycode;
CREATE TABLE IG_CONSUMER_Countycode 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT
    a.id AS id,
    MIN(b.cstatecode) AS cstatecode, 
    MIN(b.ccountycode) AS ccountycode, 
    MIN(b.ccounty) AS ccounty
FROM {new-load-table} a
LEFT JOIN 
(
    SELECT LTRIM(RTRIM(UPPER(cstatecode))) AS cstatecode, LTRIM(RTRIM(UPPER(ccountycode))) AS ccountycode, LTRIM(RTRIM(UPPER(ccounty))) AS ccounty
    FROM sql_tblstate  WHERE databaseid = 0
    GROUP BY cstatecode, ccountycode, ccounty
  ) b 
ON a.state = b.cstatecode AND a.county_code = b.ccountycode
GROUP BY a.id;

--
DROP TABLE IF EXISTS IG_CONSUMER_NEW_PreferredCity;
CREATE TABLE IG_CONSUMER_NEW_PreferredCity 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT
    a.id AS id,
    MIN(e.zipcode) AS zipcode, 
    MIN(e.city) AS city
FROM {new-load-table} a
LEFT JOIN 
(
    SELECT zipcode, city FROM {PreferredCity-load-table}
    GROUP BY zipcode, city
) E 
ON a.Zip = e.zipcode
GROUP BY a.id;


