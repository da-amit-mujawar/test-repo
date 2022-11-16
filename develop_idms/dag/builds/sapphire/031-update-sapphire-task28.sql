
/*
Script 02.1  Add Priortized SIC.sql

Just finished the 4 digit SIC priority. If possible we would like to split the 2 digit from the 4 after the 4 has been determined by priority.
We want 1 SIC per record, roll up on the SITE and individual level, SITE taking priority over individual if it gets both.
For employee size and sales, we also want to get to 1 per record, highest code takes priority. Roll up on the
SITE and individual level, SITE taking priority over individual if it gets both.

Let me know if you have any questions.

--Denise. 2014.01.14
*/

ALTER TABLE {sapphire-tbl-ctas1} ADD SIC4_Prioritized varchar(150);
ALTER TABLE {sapphire-tbl-ctas1} ADD SIC2_Prioritized varchar(2) default '';
ALTER TABLE {sapphire-tbl-ctas1} ADD EmployeeSize_Prioritized varchar(10);
ALTER TABLE {sapphire-tbl-ctas1} ADD EmployeesAtCompany_Prioritized varchar(10);
ALTER TABLE {sapphire-tbl-ctas1} ADD EmployeesCombined_Prioritized varchar(10);

ALTER TABLE {sapphire-tbl-ctas1} ADD SalesVolume_Prioritized varchar(10);
ALTER TABLE {sapphire-tbl-ctas1} ADD SalesForCompany_Prioritized varchar(10);
ALTER TABLE {sapphire-tbl-ctas1} ADD SalesCombined_Prioritized varchar(5);


ALTER TABLE {sapphire-tbl-ctas1} ADD Text_JobFunction_Rollup varchar(2) DEFAULT '';
ALTER TABLE {sapphire-tbl-ctas1} ADD Text_JobTitle_Rollup varchar(2) DEFAULT '';
ALTER TABLE {sapphire-tbl-ctas1} ADD CombinedJobTitleAndFunction_Rollup varchar(6);

ALTER TABLE  {sapphire-tbl-ctas1} add TitleGotPromoted varchar(1) DEFAULT 'N';
ALTER TABLE  {sapphire-tbl-ctas1} add SalesGrowth varchar(1) DEFAULT 'N';
ALTER TABLE  {sapphire-tbl-ctas1} add EmployeeGrowth varchar(1);
ALTER TABLE {sapphire-tbl-ctas1} ADD Industry_Prioritized varchar(6) DEFAULT '';

Drop table IF EXISTS #SICpriorityTable;
CREATE TABLE #SICpriorityTable  (Priority int, SIC4 VARCHAR (4), cDesc VARCHAR(70));

COPY #SICpriorityTable
    FROM 's3://{s3-internal}{s3-key-buildsupportfiles}/Sapphire SIC Priority.txt'
    iam_role '{iam}'
    ACCEPTINVCHARS FILLRECORD
    delimiter '|'
    ignoreheader as 1;

/*--Delete blanks*/
DELETE from #SICpriorityTable  where (SIC4 is null OR SIC4='');
UPDATE #SICpriorityTable  SET SIC4 = LTRIM(RTRIM(UPPER(SIC4)));

Drop table IF EXISTS Temp_SIC_Company;
CREATE TABLE Temp_SIC_Company (ListID INT, Individual_ID VARCHAR(17), Company_MC VARCHAR(15), SIC4 VARCHAR(150));

/*--Step 1: Create Master SIC4 Table.  don't pull SICs if they are not listed in txt file.*/
INSERT INTO Temp_SIC_Company (ListID, Individual_ID, Company_MC, SIC4 )
SELECT DISTINCT ListID, Individual_ID, Company_MC, SIC4
  FROM {sapphire-tbl-ctas1}
where (SIC4 <>'' AND SIC4 is NOT NULL)
AND ltrim(rtRim(SIC4)) IN (SELECT DISTINCT SIC4 from #SICpriorityTable  );

/*--Step 2: Create a Priority*/
ALTER TABLE Temp_SIC_Company ADD Priority int;
UPDATE Temp_SIC_Company
   SET Priority = SICPriorityTable.Priority
  FROM Temp_SIC_Company a
 INNER JOIN #SICpriorityTable SICPriorityTable on  a.SIC4 = SICPriorityTable.SIC4;

/*-- Get Rank for Company MC */
DROP table IF EXISTS #OnePerSIC_Company;
CREATE TABLE #OnePerSIC_Company (Company_MC VARCHAR(15), SIC4 VARCHAR(150));
INSERT INTO #OnePerSIC_Company  (Company_MC,SIC4)
  SELECT Company_MC,SIC4
  FROM (SELECT *,RANK() OVER (PARTITION BY RankTable.company_mc  ORDER BY RankTable.Priority ASC) AS RankColumn FROM Temp_SIC_Company as RankTable) as ABC
WHERE RankColumn = 1
GROUP by Company_MC,SIC4;

/* -- Get Rank for Individual ID*/
DROP table  IF EXISTS #OnePerSIC_IndividualID;
CREATE TABLE #OnePerSIC_IndividualID (
    Individual_ID VARCHAR(17),
     SIC4 VARCHAR(150))
     DISTSTYLE key distkey ( individual_id )
     sortkey (individual_id);

INSERT INTO  #OnePerSIC_IndividualID (Individual_ID,SIC4)
  SELECT Individual_ID,SIC4
  FROM (SELECT *,RANK() OVER (PARTITION BY RankTable.Individual_ID  ORDER BY RankTable.Priority ASC) AS RankColumn FROM Temp_SIC_Company as RankTable) as ABC
WHERE RankColumn = 1
GROUP by Individual_ID,SIC4;

/*--Now, table is ready. Update the tblMainTable.  */
DROP TABLE IF EXISTS {sapphire-update1-ctas};
CREATE TABLE {sapphire-update1-ctas} as
    select id,
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
               from {sapphire-tbl-ctas1};

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET SIC4_Prioritized = OnePerSIC.SIC4
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN #OnePerSIC_Company  OnePerSIC ON tblMain.Company_MC = OnePerSIC.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET SIC4_Prioritized = OnePerSIC.SIC4
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN #OnePerSIC_IndividualID  OnePerSIC On tblMain.Individual_ID = OnePerSIC.Individual_ID
 WHERE tblMain.SIC4_Prioritized IS NULL;

/*--Remaining to ''*/
UPDATE {sapphire-update1-ctas}
   SET SIC4_Prioritized = ''
  FROM {sapphire-update1-ctas} tblMain
 WHERE SIC4_Prioritized IS NULL;

/* Now Create a SIC2 Priority  field*/
UPDATE {sapphire-update1-ctas}
SET SIC2_Prioritized = LEFT(SIC4_Prioritized,2)
FROM {sapphire-update1-ctas} tblMain WHERE SIC4_Prioritized <>'';

/*--Now Drop the temp Tables*/
DROP TABLE IF EXISTS Temp_SIC_Company;
DROP TABLE IF EXISTS #OnePerSIC_Company;
DROP TABLE IF EXISTS #OnePerSIC_IndividualID;
DROP TABLE IF EXISTS #SICpriorityTable ;

/*
--Fix
There are however some large company locations that end up with obviously bad SICs.
We knew this would happen to some extent as the input is imperfect-
but I am hoping that if I provide a list of Company Match Keys and the SIC 4 & 2 that we want to use, that you could replace the SICs.
 Is this a reasonable possibility? Probably less than 100 company match keys involved.
We would need to do this every time- and if not this time, we will want to do it next time.
--Denise 2014.02.27
*/

/*
if file not found, copy 'Company MC Pipe SIC4 Priority.txt'  from \Decode Data
to {USERVAR(PATH_IDMSFILES)}\Sapphire\BuildSupportFiles
*/

DROP TABLE IF EXISTS #CompanySICs;
CREATE TABLE #CompanySICs (COMPANY_MC VARCHAR(15), SIC4 VARCHAR (4));
COPY #CompanySICs
    FROM 's3://{s3-internal}{s3-key-buildsupportfiles}/Company MC Pipe SIC4 Priority.txt'
    iam_role '{iam}'
    ACCEPTINVCHARS FILLRECORD
    delimiter '|'
    ignoreheader as 1;
Delete from #CompanySICs where COMPANY_MC ='' or SIC4 ='';
Update #CompanySICs  SET SIC4 = UPPER(LTRIM(RTRIM(SIC4)));

UPDATE {sapphire-update1-ctas}
   SET SIC4_Prioritized = B.SIC4,
       SIC2_Prioritized = LEFT(B.SIC4,2)
    FROM {sapphire-update1-ctas} tblMain
 INNER JOIN #CompanySICs  B ON UPPER(tblMain.Company_MC) = UPPER(B.Company_MC);
DROP TABLE if EXISTS #CompanySICs;

/*Script 02.2  Add Priortized Emp and Sales codes*/

/*
For employee size and sales, we also want to get to 1 per record, ?highest? code takes priority. Roll up on the
SITE and individual level, SITE taking priority over individual if it gets both.
--Denise. 2014.01.14
*/

/*
Select *
into {TASK(1|Result.Parameter.Value|MainTableName)}_ToBeDropped
from {TASK(1|Result.Parameter.Value|MainTableName)};
commit;

*/

CREATE TABLE SapphireCompanyLevel (Company_MC varchar(15), filler1 varchar(2), DecodeValue varchar(10), filler2 varchar(2));
CREATE TABLE SapphireIndividualLevel (Individual_ID varchar(17), DecodeValue varchar(10), filler1 varchar(2));
CREATE TABLE SapphireCompanyLevel_DeDup (Company_MC varchar(15), DecodeValue varchar(10));
CREATE TABLE SapphireIndividualLevel_Dedup (
    Individual_ID varchar(17), DecodeValue varchar(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

COPY SapphireIndividualLevel
    (individual_id, decodevalue, filler1)
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeeSize_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';

COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeeSize_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireCompanyLevel_DeDup  (Company_MC,DecodeValue)
  SELECT Company_MC,MAX(DecodeValue) As DecodeValue
  FROM  SapphireCompanyLevel
GROUP by Company_MC;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireIndividualLevel_DeDup  (Individual_ID,DecodeValue)
  SELECT Individual_ID,MAX(DecodeValue) As DecodeValue
  FROM  SapphireIndividualLevel
GROUP by Individual_ID;

/*--Now, table is ready. Update the tblMainTable.*/

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET EmployeeSize_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireCompanyLevel_DeDup  DedupTable ON tblMain.Company_MC = DedupTable.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET EmployeeSize_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireIndividualLevel_DeDup  DedupTable ON tblMain.Individual_ID = DedupTable.Individual_ID
 WHERE tblMain.EmployeeSize_Prioritized IS NULL;

DROP TABLE IF EXISTS SapphireIndividualLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel_DeDup ;
DROP TABLE IF EXISTS SapphireIndividualLevel_Dedup;

/*--EmployeesAtCompany_Prioritized*/
CREATE TABLE SapphireCompanyLevel (Company_MC varchar(15), filler1 varchar(2), DecodeValue varchar(10), filler2 varchar(2));
CREATE TABLE SapphireIndividualLevel (Individual_ID varchar(17), DecodeValue varchar(10), filler1 varchar(2));
CREATE TABLE SapphireCompanyLevel_DeDup (Company_MC varchar(15), DecodeValue varchar(10));
CREATE TABLE SapphireIndividualLevel_Dedup (
    Individual_ID varchar(17), DecodeValue varchar(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeesAtCompany_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';

COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeesAtCompany_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireCompanyLevel_DeDup  (Company_MC,DecodeValue)
  SELECT Company_MC,MAX(DecodeValue) As DecodeValue
  FROM  SapphireCompanyLevel
GROUP by Company_MC
;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireIndividualLevel_DeDup  (Individual_ID,DecodeValue)
  SELECT Individual_ID,MAX(DecodeValue) As DecodeValue
  FROM  SapphireIndividualLevel
GROUP by Individual_ID
;

/*
to re-do
UPDATE {TASK(1|Result.Parameter.Value|MainTableName)}  SET EmployeesAtCompany_Prioritized = Null; commit;
*/

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET EmployeesAtCompany_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireCompanyLevel_DeDup  DedupTable ON tblMain.Company_MC = DedupTable.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET EmployeesAtCompany_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireIndividualLevel_DeDup  DedupTable ON tblMain.Individual_ID = DedupTable.Individual_ID
 WHERE tblMain.EmployeesAtCompany_Prioritized IS NULL;

DROP TABLE IF EXISTS SapphireIndividualLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel_DeDup ;
DROP TABLE IF EXISTS SapphireIndividualLevel_Dedup;

/*--Special Logic  for --EmployeesCombined_Prioritized*/
/*
the Employees At Company numbers didnt come out how I was hoping and I am wondering if could use the same logic 
but combine 2 fields Employee Size and Employees At Company. You would have to drop the first letter of the code- but then they would match up.
Or could you do this in another field and still get to one per individual using company as a priority?
--Denise
*/
CREATE TABLE SapphireCompanyLevel (Company_MC varchar(15), filler1 varchar(2), DecodeValue varchar(10), filler2 varchar(2));
CREATE TABLE SapphireIndividualLevel (Individual_ID varchar(17), DecodeValue varchar(10), filler1 varchar(2));
CREATE TABLE SapphireCompanyLevel_DeDup (Company_MC varchar(15), DecodeValue varchar(10));
CREATE TABLE SapphireIndividualLevel_Dedup (
    Individual_ID varchar(17), DecodeValue varchar(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeeSize_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';
COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeesAtCompany_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';

COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeeSize_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_EmployeesAtCompany_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

/*--Now we gor 2 tables with employee and Employeecombined together.*/
/*--As per logic, remove the first char*/
UPDATE SapphireIndividualLevel  SET DecodeValue = LTRIM(RTRIM(substring(DecodeValue,2,9))) ;
UPDATE SapphireCompanyLevel  SET DecodeValue = LTRIM(RTRIM(substring(DecodeValue,2,9))) ;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireCompanyLevel_DeDup  (Company_MC,DecodeValue)
  SELECT Company_MC,MAX(DecodeValue) As DecodeValue
  FROM  SapphireCompanyLevel
GROUP by Company_MC;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireIndividualLevel_DeDup  (Individual_ID,DecodeValue)
  SELECT Individual_ID,MAX(DecodeValue) As DecodeValue
  FROM  SapphireIndividualLevel
GROUP by Individual_ID;

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET EmployeesCombined_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireCompanyLevel_DeDup  DedupTable ON tblMain.Company_MC = DedupTable.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET EmployeesCombined_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireIndividualLevel_DeDup  DedupTable ON tblMain.Individual_ID = DedupTable.Individual_ID
 WHERE tblMain.EmployeesCombined_Prioritized IS NULL;

DROP TABLE IF EXISTS SapphireIndividualLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel_DeDup ;
DROP TABLE IF EXISTS SapphireIndividualLevel_Dedup;

/*--SalesVolume_Prioritized*/
CREATE TABLE SapphireCompanyLevel (Company_MC varchar(15), filler1 varchar(2), DecodeValue varchar(10), filler2 varchar(2));
CREATE TABLE SapphireIndividualLevel (Individual_ID varchar(17), DecodeValue varchar(10), filler1 varchar(2));
CREATE TABLE SapphireCompanyLevel_DeDup (Company_MC varchar(15), DecodeValue varchar(10));
CREATE TABLE SapphireIndividualLevel_Dedup (
    Individual_ID varchar(17), DecodeValue varchar(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesVolume_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';
COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesVolume_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireCompanyLevel_DeDup  (Company_MC,DecodeValue)
  SELECT Company_MC,MAX(DecodeValue) As DecodeValue
  FROM  SapphireCompanyLevel
GROUP by Company_MC;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireIndividualLevel_DeDup  (Individual_ID,DecodeValue)
  SELECT Individual_ID,MAX(DecodeValue) As DecodeValue
  FROM  SapphireIndividualLevel
GROUP by Individual_ID;

/*--Now, table is ready. Update the tblMainTable.*/

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET SalesVolume_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireCompanyLevel_DeDup  DedupTable ON tblMain.Company_MC = DedupTable.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET SalesVolume_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireIndividualLevel_DeDup  DedupTable ON tblMain.Individual_ID = DedupTable.Individual_ID
 WHERE tblMain.SalesVolume_Prioritized IS NULL;

DROP TABLE IF EXISTS SapphireIndividualLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel_DeDup ;
DROP TABLE IF EXISTS SapphireIndividualLevel_Dedup;

/*-- SalesForCompany_Prioritized*/
CREATE TABLE SapphireCompanyLevel (Company_MC varchar(15), filler1 varchar(2), DecodeValue varchar(10), filler2 varchar(2));
CREATE TABLE SapphireIndividualLevel (Individual_ID varchar(17), DecodeValue varchar(10), filler1 varchar(2));
CREATE TABLE SapphireCompanyLevel_DeDup (Company_MC varchar(15), DecodeValue varchar(10));
CREATE TABLE SapphireIndividualLevel_Dedup (
    Individual_ID varchar(17), DecodeValue varchar(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesForCompany_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';
COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesForCompany_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireCompanyLevel_DeDup  (Company_MC,DecodeValue)
  SELECT Company_MC,MAX(DecodeValue) As DecodeValue
  FROM  SapphireCompanyLevel
GROUP by Company_MC;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireIndividualLevel_DeDup  (Individual_ID,DecodeValue)
  SELECT Individual_ID,MAX(DecodeValue) As DecodeValue
  FROM  SapphireIndividualLevel
GROUP by Individual_ID;

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET SalesForCompany_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireCompanyLevel_DeDup  DedupTable ON tblMain.Company_MC = DedupTable.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET SalesForCompany_Prioritized = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireIndividualLevel_DeDup  DedupTable ON tblMain.Individual_ID = DedupTable.Individual_ID
 WHERE tblMain.SalesForCompany_Prioritized IS NULL;

DROP TABLE IF EXISTS SapphireIndividualLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel_DeDup ;
DROP TABLE IF EXISTS SapphireIndividualLevel_Dedup;

--Add SalesCombined_Prioritized.  Titcket #155576
CREATE TABLE SapphireCompanyLevel (Company_MC varchar(15), filler1 varchar(2), DecodeValue varchar(10), filler2 varchar(2));
CREATE TABLE SapphireIndividualLevel (Individual_ID varchar(17), DecodeValue varchar(10), filler1 varchar(2));
CREATE TABLE SapphireCompanyLevel_DeDup (Company_MC varchar(15), DecodeValue varchar(10));
CREATE TABLE SapphireIndividualLevel_Dedup (
    Individual_ID varchar(17), DecodeValue varchar(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesVolume_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';
COPY SapphireIndividualLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesForCompany_IND.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'individual_id:17, decodevalue:10, filler1:2';

COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesVolume_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';
COPY SapphireCompanyLevel
    FROM 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_SalesForCompany_COMP.txt' --param
    iam_role '{iam}'
    ACCEPTINVCHARS
    fixedwidth 'Company_MC:15, filler1:2, decodevalue:10, filler2:2';

/*--Now we gor 2 tables with employee and Employeecombined together.*/
/*--As per logic, remove the first char*/
UPDATE SapphireIndividualLevel  SET DecodeValue = LTRIM(RTRIM(substring(DecodeValue,2,9))) ;
UPDATE SapphireCompanyLevel  SET DecodeValue = LTRIM(RTRIM(substring(DecodeValue,2,9))) ;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireCompanyLevel_DeDup  (Company_MC,DecodeValue)
  SELECT Company_MC,MAX(DecodeValue) As DecodeValue
  FROM  SapphireCompanyLevel
GROUP by Company_MC;

/*--Dedup.  Get the MAX Decoded value, by company*/
INSERT INTO SapphireIndividualLevel_DeDup  (Individual_ID,DecodeValue)
  SELECT Individual_ID,MAX(DecodeValue) As DecodeValue
  FROM  SapphireIndividualLevel
GROUP by Individual_ID;

/*--First Priority is Company_MC match*/
UPDATE {sapphire-update1-ctas}
   SET SalesCombined_Prioritized  = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireCompanyLevel_DeDup  DedupTable ON tblMain.Company_MC = DedupTable.Company_MC;

/*--Second Priority is Individual_ID match*/
UPDATE {sapphire-update1-ctas}
   SET SalesCombined_Prioritized  = LTRIM(RTRIM(UPPER(DedupTable.DecodeValue)))
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN SapphireIndividualLevel_DeDup  DedupTable ON tblMain.Individual_ID = DedupTable.Individual_ID
 WHERE tblMain.SalesForCompany_Prioritized IS NULL;

DROP TABLE IF EXISTS SapphireIndividualLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel;
DROP TABLE IF EXISTS SapphireCompanyLevel_DeDup ;
DROP TABLE IF EXISTS SapphireIndividualLevel_Dedup;
