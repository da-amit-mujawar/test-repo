DROP TABLE IF EXISTS {DnB-stage-table};

CREATE TABLE {DnB-stage-table}
DISTSTYLE ALL
AS
SELECT
     a.id AS id,
     a.dbus_duns AS dbus_duns,
     UPPER(a.dbus_businessname) AS dbus_businessname,
     UPPER(a.dbus_tradestylename) AS dbus_tradestylename,
     UPPER(a.dbus_streetaddress) AS dbus_streetaddress,
     UPPER(a.dbus_city) AS dbus_city,
     UPPER(a.dbus_stateabbreviation) AS dbus_stateabbreviation,
     UPPER(a.dbus_zipcodeforstreetaddress) AS dbus_zipcodeforstreetaddress,
     UPPER(a.dbus_zip_4extentionforstreetaddress) AS dbus_zip_4extentionforstreetaddress,
     UPPER(a.dbus_mailingaddress) AS dbus_mailingaddress,
     UPPER(a.dbus_mailingcityname) AS dbus_mailingcityname,
     UPPER(a.dbus_mailingstateabbreviation) AS dbus_mailingstateabbreviation,
     UPPER(a.dbus_mailingzipcode) AS dbus_mailingzipcode,
     UPPER(a.dbus_mailingzip_4extention) AS dbus_mailingzip_4extention,
     UPPER(a.dbus_carrierroutecode) AS dbus_carrierroutecode,
     UPPER(a.dbus_deliverypointpostalcode) AS dbus_deliverypointpostalcode,
     UPPER(a.dbus_nationalcodes) AS dbus_nationalcodes,
     UPPER(a.dbus_dunandbradstreetstatecode) AS dbus_dunandbradstreetstatecode,
     UPPER(a.dbus_dunandbradstreetcountycode) AS dbus_dunandbradstreetcountycode,
     UPPER(a.dbus_dunandbradstreetcitycode) AS dbus_dunandbradstreetcitycode,
     UPPER(a.dbus_dunandbradstreetsmsacode) AS dbus_dunandbradstreetsmsacode,
     UPPER(a.dbus_telephonenbr) AS dbus_telephonenbr,
     UPPER(a.dbus_ceoname) AS dbus_ceoname,
     UPPER(a.dbus_ceotitle) AS dbus_ceotitle,
     UPPER(a.dbus_salesvolume) AS dbus_salesvolume,
     UPPER(a.dbus_codeforestimateorrange1) AS dbus_codeforestimateorrange1,
     UPPER(a.dbus_employeescompany) AS dbus_employeescompany,
     UPPER(a.dbus_codeforestimateorrange2) AS dbus_codeforestimateorrange2,
     UPPER(a.dbus_employeeslocation) AS dbus_employeeslocation,
     UPPER(a.dbus_codeforestimateorrange3) AS dbus_codeforestimateorrange3,
     UPPER(a.dbus_yearstarted) AS dbus_yearstarted,
     UPPER(a.dbus_statusindicator) AS dbus_statusindicator,
     UPPER(a.dbus_populationcode) AS dbus_populationcode,
     UPPER(a.dbus_transactioncodes) AS dbus_transactioncodes,
     UPPER(a.dbus_lastupdatedate) AS dbus_lastupdatedate,
     UPPER(a.dbus_cottageind) AS dbus_cottageind,
     UPPER(a.dbus_smallbusiness) AS dbus_smallbusiness,
     UPPER(a.dbus_womenowned) AS dbus_womenowned,
     UPPER(a.dbus_minorityownedind) AS dbus_minorityownedind,
     UPPER(a.dbus_minorityclassif) AS dbus_minorityclassif,
     UPPER(a.dbus_import_export) AS dbus_import_export,
     UPPER(a.dbus_filler12) AS dbus_filler12,
     UPPER(a.dbus_recordclasstype) AS dbus_recordclasstype,
     UPPER(a.dbus_lineofbusiness)  AS dbus_lineofbusiness,
     UPPER(a.dbus_sic) AS dbus_sic,
     UPPER(a.dbus_dunandbradstreet2_2extention1) AS dbus_dunandbradstreet2_2extention1,
     UPPER(a.dbus_dunandbradstreet2_2extention2) AS dbus_dunandbradstreet2_2extention2,
     UPPER(a.dbus_dunandbradstreet2_2extention3) AS dbus_dunandbradstreet2_2extention3,
     UPPER(a.dbus_dunandbradstreet2_2extention4) AS dbus_dunandbradstreet2_2extention4,
     UPPER(a.dbus_sic2sameformatasprimarysic) AS dbus_sic2sameformatasprimarysic,
     UPPER(a.dbus_sic3sameformatasprimarysic) AS dbus_sic3sameformatasprimarysic,
     UPPER(a.dbus_sic4sameformatasprimarysic) AS dbus_sic4sameformatasprimarysic,
     UPPER(a.dbus_sic5sameformatasprimarysic) AS dbus_sic5sameformatasprimarysic,
     UPPER(a.dbus_sic6sameformatasprimarysic) AS dbus_sic6sameformatasprimarysic,
     UPPER(a.dbus_franchisecode)  AS dbus_franchisecode,
     UPPER(a.dbus_franchisecode2) AS dbus_franchisecode2,
     UPPER(a.dbus_franchisecode3) AS dbus_franchisecode3,
     UPPER(a.dbus_franchisecode4) AS dbus_franchisecode4,
     UPPER(a.dbus_franchisecode5) AS dbus_franchisecode5,
     UPPER(a.dbus_franchisecode6) AS dbus_franchisecode6,
     UPPER(a.dbus_franchiseind) AS dbus_franchiseind,
     UPPER(a.dbus_faxnumber) AS dbus_faxnumber,
     UPPER(a.dbus_public_privateind) AS dbus_public_privateind,
     UPPER(a.dbus_wholesaleofficesupplieswosbi) AS dbus_wholesaleofficesupplieswosbi,
     UPPER(a.dbus_fortune1000) AS dbus_fortune1000,
     UPPER(a.dbus_percentgrowthsales3yr) AS dbus_percentgrowthsales3yr,
     UPPER(a.dbus_percentgrowthemploy3yr) AS dbus_percentgrowthemploy3yr,
     UPPER(a.dbus_trendyearsales3yr) AS dbus_trendyearsales3yr,
     UPPER(a.dbus_trendyearemploy3yr) AS dbus_trendyearemploy3yr,
     UPPER(a.dbus_percentgrowthsales5yr) AS dbus_percentgrowthsales5yr,
     UPPER(a.dbus_percentgrowthemploy5yr) AS dbus_percentgrowthemploy5yr,
     UPPER(a.dbus_trendyearsales5yr) AS dbus_trendyearsales5yr,
     UPPER(a.dbus_trendyearemploy5yr)AS dbus_trendyearemploy5y,
     UPPER(a.dbus_baseyearsales) AS dbus_baseyearsales,
     UPPER(a.dbus_baseyearemployees) AS dbus_baseyearemployees,
     UPPER(a.dbus_headquarterduns) AS dbus_headquarterduns,
     UPPER(a.dbus_parentduns) AS dbus_parentduns,
     UPPER(a.dbus_domesticultimateduns) AS dbus_domesticultimateduns,
     UPPER(a.dbus_ownsrents) AS dbus_ownsrents,
     UPPER(a.dbus_squarefootage) AS dbus_squarefootage,
     UPPER(a.dbus_legalstatuscode) AS dbus_legalstatuscode,
     UPPER(a.dbus_numberoffamilymembers) AS dbus_numberoffamilymembers,
     UPPER(a.dbus_estimatedpcs) AS dbus_estimatedpcs,
     UPPER(a.dbus_estimatedprinters) AS dbus_estimatedprinters,
     UPPER(a.dbus_estimatednumberofcopiers) AS dbus_estimatednumberofcopiers,
     CASE WHEN LTRIM(RTRIM(a.dbus_telephonenbr)) <>'' THEN 'Y' ELSE a.dbus_hasphone END AS dbus_hasphone,
     CASE WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 1 AND 4 THEN 'A'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 5 AND 9 THEN 'B'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 10 AND 19 THEN 'C'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 20 AND 49 THEN 'D'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 50 AND 99 THEN 'E'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 100 AND 249 THEN 'F'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 250 AND 499 THEN 'G'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 500 AND 999 THEN 'H'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 1000 AND 4999 THEN 'I'
          WHEN CAST (a.dbus_employeescompany AS INT ) BETWEEN 5000 AND 9999 THEN 'J'
          WHEN CAST (a.dbus_employeescompany AS INT ) > 10000  THEN 'K'
          END AS employeescompany_recoded,
     CASE WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 1 AND 4 THEN 'A'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 5 AND 9 THEN 'B'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 10 AND 19 THEN 'C'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 20 AND 49 THEN 'D'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 50 AND 99 THEN 'E'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 100 AND 249 THEN 'F'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 250 AND 499 THEN 'G'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 500 AND 999 THEN 'H'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 1000 AND 4999 THEN 'I'
          WHEN CAST (a.dbus_employeeslocation AS INT ) BETWEEN 5000 AND 9999 THEN 'J'
          WHEN CAST (a.dbus_employeeslocation AS INT ) > 10000  THEN 'K'
          END AS dbus_employeeslocation_recoded,
     CASE WHEN CAST (a.dbus_salesvolume AS BIGINT )/1000 BETWEEN 1 AND 499 THEN 'A'
          WHEN CAST (a.dbus_salesvolume AS BIGINT )/1000 BETWEEN 500 AND 999 THEN 'B'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 1 AND 2.499 THEN 'C'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 2.5 AND 4.999 THEN 'D'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 5 AND 9.999 THEN 'E'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 10 AND 19.999 THEN 'F'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 20 AND 49.999 THEN 'G'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 50 AND 99.999 THEN 'H'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 100 AND 499 THEN 'I'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000 BETWEEN 500 AND 999 THEN 'J'
          WHEN CAST (a.dbus_salesvolume AS BIGINT ) /1000000  > 10000  THEN 'K' 
          END AS dbus_salesvolume_recoded,
     LTRIM(RTRIM(a.dbus_sic)) + LEFT(LTRIM(RTRIM(a.dbus_dunandbradstreet2_2extention1)),2) AS dbus_sic6,
     LTRIM(RTRIM(a.dbus_sic)) +  LEFT(LTRIM(RTRIM(a.dbus_dunandbradstreet2_2extention1)),4) AS dbus_sic8,
     GetMatchCode('', '',REPLACE(a.dbus_streetaddress,'|',''), REPLACE(REPLACE(a.dbus_zipcodeforstreetaddress,'|',''),' ', ''), REPLACE(a.dbus_businessname,'|',''),'C') AS company_mc
FROM {DnB-raw-table} a
WHERE a.id IN 
(
     SELECT MAX(id) AS id FROM {DnB-raw-table} 
     GROUP BY GetMatchCode('', '', dbus_streetaddress, dbus_zipcodeforstreetaddress, dbus_businessname,'C')
);



--Rename the stage table
DROP TABLE IF EXISTS {maintable_name} CASCADE;
ALTER TABLE {DnB-stage-table} RENAME TO {maintable_name};
