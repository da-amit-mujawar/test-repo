/*--Text title and function roll up for Sapphire*/
CREATE TABLE #SapphireFunctionRollups (cValue varchar(5), DecodeValue varchar(2));
CREATE TABLE #SapphireTitleRollups (cValue varchar(5), DecodeValue varchar(2));

copy #SapphireFunctionRollups
from 's3://{s3-internal}{s3-key-buildsupportfiles}/Text Function Roll up pipe.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;
copy #SapphireTitleRollups
from 's3://{s3-internal}{s3-key-buildsupportfiles}/Text Title Roll up pipe.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;


Update #SapphireFunctionRollups SET cValue = UPPER(LTRIM(RTRIM(cValue))), DecodeValue = UPPER(LTRIM(RTRIM(DecodeValue)));
Update #SapphireTitleRollups SET cValue = UPPER(LTRIM(RTRIM(cValue))), DecodeValue = UPPER(LTRIM(RTRIM(DecodeValue)));
DELETE FROM #SapphireFunctionRollups WHERE DecodeValue='';
DELETE FROM #SapphireTitleRollups WHERE DecodeValue='';
DELETE FROM #SapphireFunctionRollups WHERE cValue='Z9Z';
DELETE FROM #SapphireTitleRollups WHERE cValue='Z9Z';

-- UPDATE {sapphire-tbl-ctas1}
-- SET Text_JobFunction_Rollup = '',
--     Text_JobTitle_Rollup  = '' ; -- PUT THIS IN CTAS

UPDATE {sapphire-update1-ctas}
  SET Text_JobFunction_Rollup = B.DecodeValue
From {sapphire-update1-ctas} A
Inner Join #SapphireFunctionRollups B  on UPPER(LTRIM(RTRIM(A.Text_JobFunction1))) = B.cValue
where A.Text_JobFunction1 <>'';

/*-- update --Text Function Roll up*/
UPDATE {sapphire-update1-ctas}
  SET Text_JobTitle_Rollup = B.DecodeValue
From {sapphire-update1-ctas} A
Inner Join #SapphireTitleRollups B  on UPPER(LTRIM(RTRIM(A.Text_JobTitle))) = B.cValue
where A.Text_JobTitle <>'';

DROP TABLE IF EXISTS  #SapphireFunctionRollups;
DROP TABLE IF EXISTS #SapphireTitleRollups;

DROP TABLE IF EXISTS SapphireTextTitleFunctionCombineRollupLookup;
CREATE TABLE SapphireTextTitleFunctionCombineRollupLookup
(Text_JobTitle_Rollup varchar(15), Text_JobFunction_Rollup varchar(15),CombinedJobTitleAndFunction_Rollup varchar(15) );
copy SapphireTextTitleFunctionCombineRollupLookup
from 's3://{s3-internal}{s3-key-buildsupportfiles}/TextTitleFunctionCombineRollupPIPE.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

/*--clean Up*/
Update SapphireTextTitleFunctionCombineRollupLookup
    SET Text_JobTitle_Rollup =  CASE WHEN Text_JobTitle_Rollup is Null OR Upper(ltrim(rtrim(Text_JobTitle_Rollup))) ='NULL'  then '' Else Upper(ltrim(rtrim(Text_JobTitle_Rollup))) END,
    Text_JobFunction_Rollup =  CASE WHEN Text_JobFunction_Rollup is Null OR Upper(ltrim(rtrim(Text_JobFunction_Rollup))) ='NULL' then '' Else Upper(ltrim(rtrim(Text_JobFunction_Rollup))) END,
    CombinedJobTitleAndFunction_Rollup =  CASE WHEN CombinedJobTitleAndFunction_Rollup is Null OR Upper(ltrim(rtrim(CombinedJobTitleAndFunction_Rollup))) ='NULL' then '' Else Upper(ltrim(rtrim(CombinedJobTitleAndFunction_Rollup))) END;

DELETE from SapphireTextTitleFunctionCombineRollupLookup where Text_JobTitle_Rollup ='' and Text_JobFunction_Rollup ='';

/*-- update Text_JobFunction_Rollup*/
UPDATE {sapphire-update1-ctas}
  SET CombinedJobTitleAndFunction_Rollup = B.CombinedJobTitleAndFunction_Rollup
From {sapphire-update1-ctas} A
Inner Join SapphireTextTitleFunctionCombineRollupLookup B
    on UPPER(LTRIM(RTRIM(A.Text_JobTitle_Rollup))) = B.Text_JobTitle_Rollup
           AND UPPER(LTRIM(RTRIM(A.Text_JobFunction_Rollup))) = B.Text_JobFunction_Rollup;

DROP table IF EXISTS SapphireTextTitleFunctionCombineRollupLookup;

UPDATE {sapphire-update1-ctas}
SET EmployeeSize_Prioritized = CASE  WHEN EmployeeSize_Prioritized IS NULL THEN '' ELSE EmployeeSize_Prioritized END,
    EmployeesAtCompany_Prioritized = CASE  WHEN EmployeesAtCompany_Prioritized IS NULL THEN '' ELSE EmployeesAtCompany_Prioritized END,
    EmployeesCombined_Prioritized = CASE WHEN EmployeesCombined_Prioritized IS NULL THEN '' ELSE EmployeesCombined_Prioritized END,
    SalesVolume_Prioritized = CASE WHEN SalesVolume_Prioritized IS NULL THEN '' ELSE SalesVolume_Prioritized END,
    SalesForCompany_Prioritized = CASE WHEN SalesForCompany_Prioritized IS NULL THEN '' ELSE SalesForCompany_Prioritized END,
    SalesCombined_Prioritized = CASE WHEN SalesCombined_Prioritized IS NULL THEN '' ELSE SalesCombined_Prioritized END,
    CombinedJobTitleAndFunction_Rollup = CASE WHEN CombinedJobTitleAndFunction_Rollup IS NULL THEN '' ELSE CombinedJobTitleAndFunction_Rollup END;

/*
Add TitleGotPromoted.  Titcket #155576
code from excel  \Decode Data\Title got promoted.xlsx
--Reju Mathew. 2014.08.27
*/

-- ALTER TABLE  {sapphire-tbl-ctas1} add TitleGotPromoted varchar(1) DEFAULT 'N';
-- ALTER TABLE  {sapphire-tbl-ctas1} add SalesGrowth varchar(1) DEFAULT 'N';
-- ALTER TABLE  {sapphire-tbl-ctas1} add EmployeeGrowth varchar(1);
-- ALTER TABLE {sapphire-tbl-ctas1} ADD Industry_Prioritized varchar(6) DEFAULT '';

-- ANALYSE {sapphire-tbl-ctas1};

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','D19','D01','D02','D04','D05','D06','D07','D08','D10','D11','D12','D13','D14','D16','D17','D18','D20','D21','D22','D23','D24','D25','D26','D27','D28','D30','D31','D29','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')
AND B.Text_Jobtitle in ('1','131');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A01','A03','A19','E','E31');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A04');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A15','A17','A07','A13');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B01','B03','B04','B19');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B13','B15');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B16');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B17');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('C01','C03','C04','C19');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('C13','C15');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','D29','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D01','D02','D04','D05','D06','D07','D08','D10','D11','D12','D13','D14','D16','D17','D18','D20','D21','D22','D23','D24','D25','D26','D27','D28','D30','D31');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D15','N','N31');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','D01','D02','D04','D05','D06','D07','D08','D10','D11','D12','D13','D14','D16','D17','D18','D20','D21','D22','D23','D24','D25','D26','D27','D28','D30','D31','D29','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D19');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D29');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('A01','A03','A19','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31');

UPDATE {sapphire-update1-ctas}
  SET TitleGotPromoted ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
AND A.Text_Jobtitle in ('Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('ZH','ZI','ZL');


-- Update {sapphire-tbl-ctas1}
--   SET TitleGotPromoted = CASE
--       WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','D19','D01','D02','D04','D05','D06','D07','D08','D10','D11','D12','D13','D14','D16','D17','D18','D20','D21','D22','D23','D24','D25','D26','D27','D28','D30','D31','D29','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')
-- AND B.Text_Jobtitle in ('1','131') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A01','A03','A19','E','E31')
--         THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A04') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('A15','A17','A07','A13') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B01','B03','B04','B19') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B13','B15') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B16') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('B17') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('C01','C03','C04','C19') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('C13','C15') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','D29','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D01','D02','D04','D05','D06','D07','D08','D10','D11','D12','D13','D14','D16','D17','D18','D20','D21','D22','D23','D24','D25','D26','D27','D28','D30','D31') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D15','N','N31') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','D01','D02','D04','D05','D06','D07','D08','D10','D11','D12','D13','D14','D16','D17','D18','D20','D21','D22','D23','D24','D25','D26','D27','D28','D30','D31','D29','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D19') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','B01','B03','B04','B19','W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('D29') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','B13','B15','B17','B16','C01','C03','C04','C19','X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('W2','B02','B05','B06','B07','B08','B10','B11','B12','B14','B18','B20','B21','B22','B23','B24','B25','B26','B27','B28','B29','B30','B31') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('A01','A03','A19','C13','C15','D15','N','N31','E','E31','A04','A02','A05','A06','A08','A10','A11','A12','A14','A16','A18','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31','A15','A17','A07','A13','ZH','ZI','ZL','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('X02','C02','C05','C06','C07','C08','C10','C11','C12','C14','C16','C17','C18','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y')
-- AND A.Text_Jobtitle in ('Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM','X01','X05','X08','X09','ZG')  AND B.Text_Jobtitle in ('ZH','ZI','ZL') THEN 'Y'
--     WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y') THEN 'N' END,
--       SalesGrowth = CASE WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL07','CSL09','CSL10','CSL11','CSL12','CSL14','CSL15','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL06') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL09','CSL10','CSL11','CSL13','CSL14','CSL15','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL07','CSL05') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL10','CSL11','CSL13','CSL12','CSL15','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL09') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL11','CSL13','CSL12','CSL14','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL26','CSL10') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL13','CSL12','CSL14','CSL15','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL11','CSL08') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL15','CSL17','CSL18','CSL20')  AND B.SalesForCompany_Prioritized in ('CSL04','CSL14','CSL12') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL14','CSL15','CSL17','CSL18','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL13') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL17','CSL18','CSL20','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL03','CSL15') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL18','CSL20','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL17') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL20','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL18','CSL16') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
-- AND A.SalesForCompany_Prioritized in ('CSL20')  AND B.SalesForCompany_Prioritized in ('CSL19') THEN 'Y'
-- WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y') THEN 'N' END
-- FROM  {sapphire-tbl-ctas1} A
-- INNER JOIN {sapphire-pre-build}  B
--    ON A.Individual_ID = B.Individual_ID;

/*
Add EmployeeGrowth .  Titcket #155576
code from excel  \Decode Data\EmployeeGrowth.xlsx
--Reju Mathew. 2014.08.27
*/


-- Update {sapphire-update1-ctas}
-- SET EmployeeGrowth = CASE
--                          WHEN (A.EmployeeGrowth is NULL OR A.EmployeeGrowth <> 'Y')
--                              AND A.EmployeesCombined_Prioritized in
--                                  ('ES29', 'ES25', 'ES0F', 'ES24', 'ES27', 'ES23', 'ES26', 'ES21', 'ES22', 'ES20',
--                                   'ES19', 'ES0E', 'ES15', 'ES0G', 'ES12', 'ES0D', 'ES11', 'ES10', 'ES09', 'ES08',
--                                   'ES07', 'ES06') AND Individual_ID in
--                                                       (Select distinct Individual_ID from {sapphire-pre-build}
-- where EmployeesCombined_Prioritized in ('ES05', 'ES04')) THEN 'Y'
--                         WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
--                             AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10','ES09','ES08','ES07')
--                             AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES03','ES06')) THEN 'Y'
--                         WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10','ES09')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES07')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES02','ES01','ES08','ES09')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES10')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0A','ES11','ES12')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0D','ES13')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES16')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES14')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES28','ES15','ES16','ES17')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES19')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0B','ES0E','ES18','ES20')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES27','ES25','ES0F','ES24','ES23')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0C','ES22')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES21','ES23')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
-- AND A.EmployeesCombined_Prioritized in ('ES29','ES25')
-- AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES24')) THEN 'Y'
-- WHEN (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y') THEN 'N'END
-- FROM {sapphire-update1-ctas} A;

Update {sapphire-update1-ctas}
SET EmployeeGrowth = 'Y'
FROM {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10','ES09','ES08','ES07','ES06')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES05','ES04'));

Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10','ES09','ES08','ES07')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES03','ES06'));


Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10','ES09')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES07'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12','ES0D','ES11','ES10')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES02','ES01','ES08','ES09'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G','ES12')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES10'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15','ES0G')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0A','ES11','ES12'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES0E','ES15')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0D','ES13'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19','ES16')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES14'));


Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20','ES19')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES28','ES15','ES16','ES17'))
;


Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22','ES20')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES19'))
;


Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24','ES27','ES23','ES26','ES21','ES22')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0B','ES0E','ES18','ES20'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES27','ES25','ES0F','ES24','ES23')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES0C','ES22'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
AND A.EmployeesCombined_Prioritized in ('ES29','ES25','ES0F','ES24')
AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES21','ES23'));



Update {sapphire-update1-ctas}
  SET EmployeeGrowth ='Y'
from {sapphire-update1-ctas} A
  WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y')
  AND A.EmployeesCombined_Prioritized in ('ES29','ES25')
  AND Individual_ID in (Select distinct Individual_ID from {sapphire-pre-build} where EmployeesCombined_Prioritized in ('ES24'));



/*--finally*/
Update {sapphire-update1-ctas}
SET EmployeeGrowth ='N'
from {sapphire-update1-ctas} A
WHERE (A.EmployeeGrowth is NULL  OR A.EmployeeGrowth <>'Y');

/*
Add SalesGrowth .  Titcket #155576
code from excel  \Decode Data\SalesGrowth.xlsx
--Reju Mathew. 2014.08.27
*/

UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL07','CSL09','CSL10','CSL11','CSL12','CSL14','CSL15','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL06');

UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL09','CSL10','CSL11','CSL13','CSL14','CSL15','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL07','CSL05');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL10','CSL11','CSL13','CSL12','CSL15','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL09');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL11','CSL13','CSL12','CSL14','CSL17','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL26','CSL10');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL13','CSL12','CSL14','CSL15','CSL18','CSL20','CSL19','CSL02')  AND B.SalesForCompany_Prioritized in ('CSL11','CSL08');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL15','CSL17','CSL18','CSL20')  AND B.SalesForCompany_Prioritized in ('CSL04','CSL14','CSL12');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL14','CSL15','CSL17','CSL18','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL13');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL17','CSL18','CSL20','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL03','CSL15');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL18','CSL20','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL17');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL20','CSL19')  AND B.SalesForCompany_Prioritized in ('CSL18','CSL16');


UPDATE {sapphire-update1-ctas}
  SET SalesGrowth ='Y'
from {sapphire-update1-ctas} A
INNER JOIN {sapphire-pre-build}  B
   ON A.Individual_ID = B.Individual_ID
WHERE (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y')
AND A.SalesForCompany_Prioritized in ('CSL20')  AND B.SalesForCompany_Prioritized in ('CSL19');

/*--finally*/ --DEFAULT IS 'N' SO NOT NEEDED
-- UPDATE {sapphire-tbl-ctas1}
-- SET TitleGotPromoted = CASE WHEN (A.TitleGotPromoted is NULL  OR A.TitleGotPromoted <>'Y') THEN 'N' ELSE A.TitleGotPromoted END,
--     SalesGrowth = CASE WHEN (A.SalesGrowth is NULL  OR A.SalesGrowth <>'Y') THEN 'N' ELSE A.SalesGrowth END
-- from {sapphire-tbl-ctas1} A;

DROP TABLE IF EXISTS SapphireTextTitleFunctionCombineRollupLookup;

CREATE TABLE #IndustryPrioritized  (SIC4_Prioritized VARCHAR (4), Industry_Prioritized  VARCHAR(6));
copy #IndustryPrioritized
from 's3://{s3-internal}{s3-key-buildsupportfiles}/Part 1 Prioritized SIC to Industry Prioritized.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

UPDATE {sapphire-update1-ctas}
   SET Industry_Prioritized   = B.Industry_Prioritized
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN #IndustryPrioritized  B ON LTRIM(RTRIM(tblMain.SIC4_Prioritized))  = LTRIM(RTRIM(B.SIC4_Prioritized));

DROP TABLE  IF EXISTS #IndustryPrioritized;

