/*
Incident# 556567 [Resolved] RE: Park Suppression  Flag in Business Email Database /  Infogroup Business (postal)

Good morning! I have just copied over the new suppression files in the folder as requested last time, 
can you please advise on when they will be ready to use? Im afraid we can only create these once t
he new build is live and please find the chain from last time below. Please advise and if you have any questions let us know.

Apply  Park Suppresssion flag  from  50M  emails provided by Marshfield and Individual_mc|email file from Sapphire build

We already applied 50M emails from  Marshfield  +  email file from sapphire.
We have to load the same Sapphire  after we create  Individual_mc  and apply by matching Individual_mc  

{USERVAR(PATH_IDMSFILES)}Sapphire\BuildSupportFiles\
--Reju Mathew 2015.03.10


Always REPLACE  {TASK(PrevTask|StdOut)} with current tblMain 
*/

DROP TABLE IF EXISTS #ParkSuppressionFromSapphire;
DROP TABLE IF EXISTS #ParkSuppressionIndividualMC_Dist ;
DROP TABLE IF EXISTS #ParkSuppressionEmail;
DROP TABLE IF EXISTS #ParkSuppressionEmail_Dist ;


CREATE TABLE #ParkSuppressionFromSapphire (Individual_MC Varchar(25),  EmailAddress varchar(150) );
CREATE TABLE #ParkSuppressionIndividualMC_Dist (Individual_MC varchar(25) );
CREATE TABLE #ParkSuppressionEmail (EmailAddress varchar(200) );
CREATE TABLE #ParkSuppressionEmail_Dist (EmailAddress varchar(100) );


/*
--STEP1:  Email file -- Load from ParkMarketZoneUniverseLessSapphireDISTINCT
--email file File 1.  change the file name

THE COPY COMMAND WILL FAIL WITH 
"String contains invalid or unsupported UTF8 codepoints. Bad UTF8 hex sequence: 82 (error 3)" 
NEED TO CONVERT THE FILE USING TEXT CONVERTER
FOR STAGING THE FILE IS CONVERTED AND AVAILABLE ON S3
s3://idms-2722-internalfiles/Sapphire/BuildSupportFiles/ParkMarketZoneUniverseLessSapphireDISTINCT_converted.txt
*/
copy #ParkSuppressionEmail (EmailAddress)
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
fixedwidth 
'EmailAddress:200';

/*
STEP2 -- Load Individual_MC|Email
Load the file created from sapphire.  Individual_MC|Email  - change the file name. there are 4  files.

THE COPY COMMAND WILL FAIL WITH 
"String contains invalid or unsupported UTF8 codepoints. Bad UTF8 hex sequence: 82 (error 3)" 
NEED TO CONVERT THE FILE USING TEXT CONVERTER
FOR STAGING THE FILE IS CONVERTED AND AVAILABLE ON S3
s3://idms-2722-internalfiles/Sapphire/BuildSupportFiles/ParkSuppressionFromSapphire_ForBusinessEmail_converted_Part01.txt
s3://idms-2722-internalfiles/Sapphire/BuildSupportFiles/ParkSuppressionFromSapphire_ForBusinessEmail_converted_Part02.txt
s3://idms-2722-internalfiles/Sapphire/BuildSupportFiles/ParkSuppressionFromSapphire_ForBusinessEmail_converted_Part03.txt
s3://idms-2722-internalfiles/Sapphire/BuildSupportFiles/ParkSuppressionFromSapphire_ForBusinessEmail_converted_Part04.txt
*/
copy #ParkSuppressionFromSapphire (Individual_MC,EmailAddress)
from 's3://{s3-internal}{s3-key2}'
iam_role '{iam}'
delimiter '|';

/*
Select top 1000 * from #ParkSuppressionEmail;
Select top 1000 * from #ParkSuppressionFromSapphire;
*/


/*
STEP3:  Prepare Individua_MC   suppressiom
Here we are using on Individual_Mc,  not email. 
*/

DELETE from #ParkSuppressionFromSapphire  where Individual_MC in ('INDIVIDUAL_MC')   or Individual_MC =''  Or Individual_MC  is null; 

--Get Distinct Individual_MC
INSERT INTO #ParkSuppressionIndividualMC_Dist (Individual_MC)
SELECT DISTINCT Individual_MC from #ParkSuppressionFromSapphire  Where  Individual_MC <>'';

/*
STEP3:  Prepare Email suppression
Always append. We already inserted an email only file.
*/

INSERT INTO #ParkSuppressionEmail (EmailAddress)
SELECT DISTINCT EmailAddress from #ParkSuppressionFromSapphire  Where  EmailAddress <>'';


UPDATE #ParkSuppressionEmail  set EmailAddress =  LEFT(UPPER(LTRIM(RTRIM(EmailAddress))),100);
DELETE from #ParkSuppressionEmail  where EmailAddress in ('EMAILADDRESS', 'CEMAIL','EMAIL')   or EmailAddress =''  Or EmailAddress  is null;

/*
--Get the distinct emails
*/
INSERT INTO #ParkSuppressionEmail_dist (EmailAddress)
    SELECT DISTINCT EmailAddress
    from #ParkSuppressionEmail;

/*
Apply on Business email  -- this may not used after few updates.
--STEP6:  Apply by individual_MC
*/
-- HARDCODING THE TABLE NAME FOR TESTING. THIS SHOULD COME FROM DW_ADMIN QUERY LATEST BUILDFOR DATABASEID = 846
-- tblMain_{build_id}_{build} 
Update  tblMain_{build_id}_{build}    
  SET ParkSuppression  = 'Y'
From  tblMain_{build_id}_{build}  A inner join  #ParkSuppressionIndividualMC_dist  B  on A.Individual_MC =B.Individual_MC
where A.Individual_MC <>''  and (A.ParkSuppression  <> 'Y'  or A.ParkSuppression  is null) ;


/*
--STEP7:  Apply --By Email
*/
-- HARDCODING THE TABLE NAME FOR TESTING. THIS SHOULD COME FROM DW_ADMIN QUERY LATEST BUILDFOR DATABASEID = 846
-- tblMain_{build_id}_{build}
Update  tblMain_{build_id}_{build}    
  SET ParkSuppression  = 'Y'
From tblMain_{build_id}_{build}  A inner join  #ParkSuppressionEmail_dist  B  on A.Email_Address_LRFS =B.EmailAddress
where A.Email_Address_LRFS <>''  and (A.ParkSuppression  <> 'Y'  or A.ParkSuppression  is null) ;

/*
--STEP8:  Set remaing records to 'N'
*/
-- HARDCODING THE TABLE NAME FOR TESTING. THIS SHOULD COME FROM DW_ADMIN QUERY LATEST BUILDFOR DATABASEID = 846
-- tblMain_{build_id}_{build}
Update  tblMain_{build_id}_{build}  SET ParkSuppression  = 'N' where (ParkSuppression  is null);