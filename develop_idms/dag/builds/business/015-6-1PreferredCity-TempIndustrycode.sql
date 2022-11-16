DROP TABLE IF EXISTS PreferredCity_load;

CREATE TABLE PreferredCity_load
(
    filler1 VARCHAR(11),
    zipcode VARCHAR(5) UNIQUE,
    city VARCHAR(30),
    filler2 VARCHAR(9)
);

COPY PreferredCity_load
FROM 's3://idms-7933-internalfiles/FilesExportedFromIQ/lookup/OCVLPRZIP.txt'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
FIXEDWIDTH
'filler1 :11,
zipcode :5,
city :30,
filler2 :9' ;


DROP TABLE IF EXISTS PreferredCity;

CREATE TABLE PreferredCity 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT
    a.id AS id,
    MIN(e.zipcode) AS zipcode, 
    MIN(e.city) AS city
FROM tblBusinessIndividual a
LEFT JOIN 
(
    SELECT zipcode, city FROM PreferredCity_load
    GROUP BY zipcode, city
) E 
ON a.Zip_back = e.zipcode
GROUP BY a.id;

-----------------------------------------
DROP TABLE IF EXISTS tmpIndustryCode_load;
CREATE TABLE tmpIndustryCode_load
AS 
SELECT
    a.id as id,
    MIN(b.cSICCode) as cSICCode, 
    MIN(b.cIndustrySpecificCode) as cIndustrySpecificCode, 
    MIN(b.cIndustryDesc) AS cIndustryDesc
FROM tblBusinessIndividual a
LEFT JOIN 
(
   SELECT cSICCode , cIndustrySpecificCode, 
          cIndustrySpecificDescription AS cIndustryDesc
     FROM sql_tblIndustryCode
    GROUP BY cSICCode , cIndustrySpecificCode, cIndustrySpecificDescription
) b 
ON a.INDUSTRYSPECIFICFIRSTBYTE = b.cindustryspecificCode
and a.primarysic6 = b.cSICCode
GROUP BY a.id;

----------------------------------------------
DROP TABLE IF EXISTS business_tblSICCode_CLEAN;
CREATE TABLE business_tblSICCode_CLEAN 
SORTKEY(ID)
DISTKEY(ID)
AS SELECT 
A.ID AS ID,
b.cSicDescription AS PRIMARYSIC2_DESC,
C.cSicDescription AS PRIMARYSIC4_DESC
FROM tblBusinessIndividual a
left join tblSICCode B  ON a.PRIMARYSIC2 = b.cSICCode AND b.ctype ='2'
LEFT join tblSICCode C  ON a.PRIMARYSIC4 = C.cSICCode AND C.ctype ='4';

DROP TABLE IF EXISTS business_tblSICCode;
CREATE TABLE business_tblSICCode 
SORTKEY(ID)
DISTKEY(ID)
AS SELECT 
A.ID AS ID,
MIN(A.PRIMARYSIC2_DESC) AS PRIMARYSIC2_DESC,
MIN(A.PRIMARYSIC4_DESC) AS PRIMARYSIC4_DESC
FROM business_tblSICCode_CLEAN a
GROUP BY ID;

--HospitalName table
DROP TABLE IF EXISTS HospitalName_load;
CREATE TABLE HospitalName_load
(
    HospitalName Varchar(27) ,
    HospitalNumber varchar(7),
    filler_1 varchar(2)
);

COPY HospitalName_load
FROM 's3://idms-7933-internalfiles/monthly_delivery/BMIHOST'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
FIXEDWIDTH
'HospitalName :27,
HospitalNumber :7,
filler_1 :2';

DELETE from HospitalName_load where (HospitalNumber ='' ); 

DROP TABLE IF EXISTS HospitalName;
CREATE TABLE HospitalName 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT
    a.id AS id,
    MIN(k.HospitalName) AS HospitalName, 
    MIN(k.HospitalNumber) AS HospitalNumber
FROM tblBusinessIndividual a
LEFT JOIN 
(
    SELECT HospitalName, HospitalNumber FROM HospitalName_load
    GROUP BY HospitalName, HospitalNumber
) k
ON a.AMI_hospitalnumber = k.HospitalNumber
GROUP BY a.id;

