DROP TABLE IF EXISTS DDValues1 ;--5.687 sec
CREATE TABLE DDValues1 AS
SELECT
  cFieldname, 
  cValue, 
  cDescription    
from sql_tblDDDescriptions 
WHERE  databaseid =827
AND  cValue <> ''
AND cFieldname in ('DT_CORPORATEINDIVIDUAL',
                     'DT_DELIVERABILITYSCORE',
                     'DT_FILINGSTATE',
                     'DT_FILINGSTATUS',
                     'DT_FILINGTYPECODE',
                     'DT_USPSRECORDTYPE',
                     'DT_MATCHTYPE',
                     'DT_COLLATERALTYPE',
                     'IGE_TITLECODE',
                     'IG_MAILING_STATE',
                     'IG_MATCHCODE',
                     'IG_EMPLOYEESIZECODE2',
                     'IG_OUTPUTVOLUMECODE2',
                     'IG_BUSINESSSTATUSCODE',
                     'IG_COTTAGECODE',
                     'IG_OWNORLEASE',
                     'IG_SQUAREFOOTAGE8',
                     'FULFILLMENT_FLAG',
                     'IG_CBSALEVEL')
ORDER BY cFieldName, cValue;



DROP TABLE IF EXISTS ddvalues_1_dddescriptions_clean;
CREATE TABLE  ddvalues_1_dddescriptions_clean
SORTKEY(id)
DISTKEY(id)
As
select 
a.id as id,
dd1.cDescription AS DT_CorporateIndividual_Description,
dd2.cDescription AS DT_DeliverabilityScore_Description, 
dd3.cDescription AS DT_FilingState_Description,
dd4.cDescription AS DT_FilingStatus_Description,
dd5.cDescription AS DT_FilingTypeCode_Description,
dd6.cDescription AS DT_USPSRecordType_Description,
dd7.cDescription AS DT_MatchType_Description,
dd8.cDescription AS DT_CollateralType_Description, 
dd9.cDescription AS IGE_TitleCode_Description,
dd10.cDescription AS IG_Mailing_State_Description,
dd11.cDescription AS IG_MatchCode_Description,
dd12.cDescription AS IG_EmployeeSizeCode2_Description,
dd13.cDescription AS IG_OutputVolumeCode2_Description,
dd14.cDescription AS IG_BusinessStatusCode_Description, 
dd15.cDescription AS IG_CottageCode_Description,
dd16.cDescription AS IG_OwnOrLease_Description,
dd17.cDescription AS IG_Squarefootage8_Description,
dd18.cDescription AS Fulfillment_Flag_Description,
dd19.cDescription AS IG_CBSALevel_Description
FROM rr_ucc_new a
LEFT join DDValues1 DD1  ON A.DT_CorporateIndividual = DD1.cValue AND upper(DD1.cFieldname) ='DT_CORPORATEINDIVIDUAL'
LEFT join DDValues1 DD2  ON A.DT_DeliverabilityScore = DD2.cValue AND upper(DD2.cFieldname) ='DT_DELIVERABILITYSCORE'
LEFT join DDValues1 DD3  ON A.DT_FilingState = DD3.cValue AND upper(DD3.cFieldname) ='DT_FILINGSTATE'
LEFT join DDValues1 DD4  ON A.DT_FilingStatus= DD4.cValue AND upper(DD4.cFieldname) ='DT_FILINGSTATUS'
LEFT join DDValues1 DD5  ON A.DT_FilingTypeCode= DD5.cValue AND upper(DD5.cFieldname) ='DT_FILINGTYPECODE'
LEFT join DDValues1 DD6  ON A.DT_USPSRecordType= DD6.cValue AND upper(DD6.cFieldname) ='DT_USPSRECORDTYPE'
LEFT join DDValues1 DD7  ON A.DT_MatchType = DD7.cValue AND upper(DD7.cFieldname) ='DT_MATCHTYPE'
LEFT join DDValues1 DD8  ON A.DT_CollateralType = DD8.cValue AND upper(DD8.cFieldname) ='DT_COLLATERALTYPE'
LEFT join DDValues1 DD9  ON A.IGE_TitleCode = DD9.cValue AND upper(DD9.cFieldname) ='IGE_TITLECODE'
LEFT join DDValues1 DD10 ON A.IG_Mailing_State= DD10.cValue AND upper(DD10.cFieldname) ='IG_MAILING_STATE'
LEFT join DDValues1 DD11 ON A.IG_MatchCode= DD11.cValue AND upper(DD11.cFieldname) ='IG_MATCHCODE'
LEFT join DDValues1 DD12 ON A.IG_EmployeeSizeCode2= DD12.cValue AND upper(DD12.cFieldname) ='IG_EMPLOYEESIZECODE2'
LEFT join DDValues1 DD13 ON A.IG_OutputVolumeCode2 = DD13.cValue AND upper(DD13.cFieldname) ='IG_OUTPUTVOLUMECODE2'
LEFT join DDValues1 DD14 ON A.IG_BusinessStatusCode = DD14.cValue AND upper(DD14.cFieldname) ='IG_BUSINESSSTATUSCODE'
LEFT join DDValues1 DD15 ON A.IG_CottageCode= DD15.cValue AND upper(DD15.cFieldname) ='IG_COTTAGECODE'
LEFT join DDValues1 DD16 ON A.IG_OwnOrLease= DD16.cValue AND upper(DD16.cFieldname) ='IG_OWNORLEASE'
LEFT join DDValues1 DD17 ON A.IG_Squarefootage8= DD17.cValue AND upper(DD17.cFieldname) ='IG_SQUAREFOOTAGE8'
LEFT join DDValues1 DD18 ON A.Fulfillment_Flag= DD18.cValue AND upper(DD18.cFieldname) ='FULFILLMENT_FLAG'
LEFT join DDValues1 DD19 ON A.IG_CBSALevel= DD19.cValue AND upper(DD19.cFieldname) ='IG_CBSALEVEL';

DROP TABLE IF EXISTS ddvalues_1_dddescriptions;
CREATE TABLE ddvalues_1_dddescriptions 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT id AS id,
min(DT_CorporateIndividual_Description) as DT_CorporateIndividual_Description,
min(DT_DeliverabilityScore_Description) as DT_DeliverabilityScore_Description,
min(DT_FilingState_Description) as DT_FilingState_Description,
min(DT_FilingStatus_Description) as DT_FilingStatus_Description,
min(DT_FilingTypeCode_Description) as DT_FilingTypeCode_Description,
min(DT_USPSRecordType_Description) as DT_USPSRecordType_Description,
min(DT_MatchType_Description) as DT_MatchType_Description,
min(DT_CollateralType_Description) as DT_CollateralType_Description,
min(IGE_TitleCode_Description) as IGE_TitleCode_Description,
min(IG_Mailing_State_Description) as IG_Mailing_State_Description,
min(IG_MatchCode_Description) as IG_MatchCode_Description,
min(IG_EmployeeSizeCode2_Description) as IG_EmployeeSizeCode2_Description,
min(IG_OutputVolumeCode2_Description) as IG_OutputVolumeCode2_Description,
min(IG_BusinessStatusCode_Description) as IG_BusinessStatusCode_Description,
min(IG_CottageCode_Description) as IG_CottageCode_Description,
min(IG_OwnOrLease_Description) as IG_OwnOrLease_Description,
min(IG_Squarefootage8_Description) as IG_Squarefootage8_Description,
min(Fulfillment_Flag_Description) as Fulfillment_Flag_Description,
min(IG_CBSALevel_Description) as IG_CBSALevel_Description

FROM ddvalues_1_dddescriptions_clean DDValues GROUP BY ID;





DROP TABLE IF EXISTS DDValues2 ;--5.687 sec
CREATE TABLE DDValues2 AS
SELECT
  LTRIM(RTRIM(UPPER(cFieldname))) as cFieldname, 
  LTRIM(RTRIM(UPPER(cValue))) as cValue, 
  LTRIM(RTRIM(cDescription)) as cDescription    
from sql_tblDDDescriptions 
WHERE  databaseid =0 
AND LTRIM(RTRIM(UPPER(cFieldname))) in 
('STATE_2CHAR', 'STATE_2DIGIT','ETHINICITY','PHONECALLSTATUS','BUSINESSCREDITSCORE') and 
((cValue) <> '' or (cDescription) <>  '');


DROP TABLE IF EXISTS ddvalues_2_dddescriptions_clean;
CREATE TABLE  ddvalues_2_dddescriptions_clean
SORTKEY(id)
DISTKEY(id)
As
select 
a.id as id,
  DD21.cDescription AS IGE_VendorEthnicity_Description, 
  DD22.cDescription  AS IG_CreditAlphaScore_Description,
  DD23.cDescription AS IG_CallStatusCode_Description,
  DD24.cDescription AS State_Description
  
FROM rr_ucc_new a
left join DDValues2 DD21  ON A.IGE_VendorEthnicity = DD21.cValue AND LTRIM(RTRIM(UPPER(DD21.cFieldname))) ='ETHINICITY'
LEFT join DDValues2 DD22  ON A.IG_CreditAlphaScore = DD22.cValue AND LTRIM(RTRIM(UPPER(DD22.cFieldname))) ='BUSINESSCREDITSCORE'
LEFT join DDValues2 DD23  ON A.IG_CallStatusCode = DD23.cValue AND LTRIM(RTRIM(UPPER(DD23.cFieldname))) ='PHONECALLSTATUS'
LEFT join DDValues2 DD24  ON A.State= DD24.cValue AND LTRIM(RTRIM(UPPER(DD24.cFieldname))) ='STATE_2CHAR';


DROP TABLE IF EXISTS ddvalues_2_dddescriptions;
CREATE TABLE ddvalues_2_dddescriptions 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT id AS id,
min(IGE_VendorEthnicity_Description) as IGE_VendorEthnicity_Description,
min(IG_CreditAlphaScore_Description) as IG_CreditAlphaScore_Description,
min(IG_CallStatusCode_Description) as IG_CallStatusCode_Description,
min(State_Description) as State_Description

FROM ddvalues_2_dddescriptions_clean DDValues
GROUP BY ID;




























-- DROP TABLE IF EXISTS DDValues1 ;--5.687 sec
-- CREATE TABLE DDValues1 AS
-- SELECT
--   cFieldname, 
--   cValue, 
--   cDescription    
-- from sql_tblDDDescriptions 
-- WHERE  databaseid =827
-- AND  cValue <> ''
-- AND cFieldname in ('DT_CORPORATEINDIVIDUAL',
--                      'DT_DELIVERABILITYSCORE',
--                      'DT_FILINGSTATE',
--                      'DT_FILINGSTATUS',
--                      'DT_FILINGTYPECODE',
--                      'DT_USPSRECORDTYPE',
--                      'DT_MATCHTYPE',
--                      'DT_COLLATERALTYPE',
--                      'IGE_TITLECODE',
--                      'IG_MAILING_STATE',
--                      'IG_MATCHCODE',
--                      'IG_EMPLOYEESIZECODE2',
--                      'IG_OUTPUTVOLUMECODE2',
--                      'IG_BUSINESSSTATUSCODE',
--                      'IG_COTTAGECODE',
--                      'IG_OWNORLEASE',
--                      'IG_SQUAREFOOTAGE8',
--                      'FULFILLMENT_FLAG',
--                      'IG_CBSALEVEL')
-- ORDER BY cFieldName, cValue;





-- DROP TABLE IF EXISTS DDValues2 ;--5.687 sec
-- CREATE TABLE DDValues2 AS
-- SELECT
--   LTRIM(RTRIM(UPPER(cFieldname))) as cFieldname, 
--   LTRIM(RTRIM(UPPER(cValue))) as cValue, 
--   LTRIM(RTRIM(cDescription)) as cDescription    
-- from sql_tblDDDescriptions 
-- WHERE  databaseid =0 
-- AND LTRIM(RTRIM(UPPER(cFieldname))) in 
-- ('STATE_2CHAR', 'STATE_2DIGIT','ETHINICITY','PHONECALLSTATUS','BUSINESSCREDITSCORE' ) and 
-- ((cValue) <> '' or (cDescription) <>  '');