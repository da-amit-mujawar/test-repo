UPDATE {sapphire-tbl-ctas1}
SET cinclude = CASE
    WHEN cInclude ='Y' AND (EMAILADDRESS LIKE '%ABUSE@%'  OR
         EMAILADDRESS LIKE '123@%' OR
         EMAILADDRESS LIKE '1234@%' OR
         EMAILADDRESS LIKE '%@123.COM' OR
         EMAILADDRESS LIKE '%@1234.COM' OR
         EMAILADDRESS LIKE 'ABC@%' OR
         EMAILADDRESS LIKE '%ADMIN@%' OR
         EMAILADDRESS LIKE '%ADMINISTRATOR@%' OR
         EMAILADDRESS LIKE '%ANTISPAM%' OR
         EMAILADDRESS LIKE 'ASDF@%' OR
         EMAILADDRESS LIKE '%@ASDF.COM' OR
         EMAILADDRESS LIKE 'BULK@%' OR
         EMAILADDRESS LIKE 'BULKFOLDER@%' OR
         EMAILADDRESS LIKE 'BULKMAIL@%' OR
         EMAILADDRESS LIKE '%CONTACT@%' OR
         EMAILADDRESS LIKE '%@FLEETWOODASD.K12.PA.US' OR
         EMAILADDRESS LIKE 'FTP@%' OR
         EMAILADDRESS LIKE 'GARBAGE@%' OR
         EMAILADDRESS LIKE 'HELP%' OR
         EMAILADDRESS LIKE 'HOSTMASTER@%' OR
         EMAILADDRESS LIKE 'HR@%' OR
         EMAILADDRESS LIKE '%INFO@%' OR
         EMAILADDRESS LIKE '%JUNK%' OR
         EMAILADDRESS LIKE 'MAILADMIN@%' OR
         EMAILADDRESS LIKE 'MARKETING@%' OR
         EMAILADDRESS LIKE 'NEWS@%' OR
         EMAILADDRESS LIKE 'NO@%' OR
         EMAILADDRESS LIKE 'NOBODY@%' OR
         EMAILADDRESS LIKE 'NOC@%' OR
         EMAILADDRESS LIKE '%NOEMAIL%@' OR
         EMAILADDRESS LIKE 'NOMAIL@%' OR
         EMAILADDRESS LIKE 'NONE@%' OR
         EMAILADDRESS LIKE 'NOPE@%' OR
         EMAILADDRESS LIKE '%NOREPLY@%' OR
         EMAILADDRESS LIKE '%NO_SPAM%@%' OR
         EMAILADDRESS LIKE '%NOSPAM%@%' OR
         EMAILADDRESS LIKE '%NO-SPAM%@%' OR
         EMAILADDRESS LIKE 'NOT@%' OR
         EMAILADDRESS LIKE 'NOTHANKS@%' OR
         EMAILADDRESS LIKE 'NOWAY@%' OR
         EMAILADDRESS LIKE 'POSTMASTER%' OR
         EMAILADDRESS LIKE '%POSTMASTER@%' OR
         EMAILADDRESS LIKE 'PRIVACY%' OR
         EMAILADDRESS LIKE '%PRIVACY@%' OR
         EMAILADDRESS LIKE '%REPLY%@%' OR
         EMAILADDRESS LIKE '%SALES@%' OR
         EMAILADDRESS LIKE '%@SINA.COM' OR
         EMAILADDRESS LIKE 'SECURITY@%' OR
         EMAILADDRESS LIKE '%SPAM%' OR
         EMAILADDRESS LIKE '%SPAMCOP%' OR
         EMAILADDRESS LIKE 'SUPPORT@%' OR
         EMAILADDRESS LIKE '%SUPPORT@%' OR
         EMAILADDRESS LIKE 'SYSADMIN@%' OR
         EMAILADDRESS LIKE 'TRASH@%' OR
         EMAILADDRESS LIKE 'USENET@%' OR
         EMAILADDRESS LIKE 'UUCP@%' OR
         EMAILADDRESS LIKE 'WEBADMIN@%' OR
         EMAILADDRESS LIKE '%WEBADMIN@%' OR
         EMAILADDRESS LIKE 'WEBMASTER%' OR
         EMAILADDRESS LIKE '%WEBMASTER@%' OR
         EMAILADDRESS LIKE 'WWW@%' OR
         EMAILADDRESS LIKE 'XXX@%' OR
         EMAILADDRESS LIKE '%@XXX.COM' OR
         EMAILADDRESS LIKE 'XYZ@%' OR
         EMAILADDRESS LIKE '%@XYZ.COM'  ) THEN 'S'
WHEN cInclude ='Y'
AND MailabilityScore NOT IN ('1A','1C','1F','1G','1H','1Z','2C','2D','2E','2F','2G','2H','3B','3C','3F','3G','4B','4C','4D')
AND EPD_DomainType NOT IN ('A','Y','H','C','M','E','O','G','N','U','D','B','L')
AND NOT (EPD_Domain LIKE '%.COM'
  OR EPD_Domain LIKE '%.TV'
  OR EPD_Domain LIKE '%.CC'
  OR EPD_Domain LIKE '%.WS'
  OR EPD_Domain LIKE '%.INFO'
  OR EPD_Domain LIKE '%.COOP'
  OR EPD_Domain LIKE '%.AERO') THEN 'S'
WHEN cInclude IN ('Y')
AND MailabilityScore NOT IN ('1A','1C','1F','1G','1H','1Z','2C','2D','2E','2F','2G','2H','3B','3C','3F','3G','4B','4C','4D')
AND COUNTRYNAME not in ('USA', 'CANADA','')
AND  State NOT IN ('AL','AK','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','PR','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','VI','AE','AA','AE','AE','AE','AP','ON','PE','QC','SK','YT','AB','BC','MB','NB','NL','NT','NS','NU') THEN 'S' ELSE cinclude END
FROM {sapphire-tbl-ctas1};

/*  Apply Suppress\Script 2 Apply Global OO Email And domain*/
    DROP table IF EXISTS #GlobalDomains;
    DROP table IF EXISTS #GlobalEmail;

    CREATE TABLE #GlobalDomains (cDomain varchar(200));
    CREATE TABLE #Globalemail (EmailAddress varchar(200));

copy #GlobalDomains
from 's3://{s3-internal}/Sapphire/Sapphire_GlobalDomain.CSV'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter ','
ignoreheader as 1;

copy #GlobalEmail
from 's3://{s3-internal}/Sapphire/Sapphire_GlobalEmail.CSV'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter ','
ignoreheader as 1;

Delete from #GlobalDomains where cDomain ='';
Delete from #Globalemail where EmailAddress ='';

/*--Domain suppress*/
UPDATE {sapphire-tbl-ctas1}
  set cInclude = 'G'
FROM {sapphire-tbl-ctas1} a
INNER JOIN #GlobalDomains b
ON b.cDomain = a.EPD_Domain
WHERE a.cInclude = 'Y';

/*Email suppress*/
UPDATE {sapphire-tbl-ctas1}
  set cInclude = 'G'
FROM {sapphire-tbl-ctas1} a
INNER JOIN #Globalemail b
ON b.EmailAddress = a.EmailAddress
WHERE a.cInclude = 'Y';

/*
Apply OO and HH emails from outside feed (Yesmail)  on sapphire
*/

DROP table IF EXISTS #OptoutsAndHardbounce;
CREATE TABLE #OptoutsAndHardbounce (EmailAddress varchar(200), cInclude char(1));

copy #OptoutsAndHardbounce
from 's3://{s3-internal}/Sapphire/Sapphire_OptoutHardBounce.CSV'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

Delete from #OptoutsAndHardbounce where EmailAddress ='';
UPDATE #OptoutsAndHardbounce  SET EmailAddress = UPPER(LEFT(LTRIM(RTRIM(EmailAddress)), 65)), cInclude = UPPER(cInclude);

/*--Set cInclude*/
UPDATE {sapphire-tbl-ctas1}
  set cInclude = b.cInclude
FROM {sapphire-tbl-ctas1} A
INNER JOIN #OptoutsAndHardbounce b ON A.EmailAddress = B.EmailAddress
WHERE A.cInclude = 'Y';

/* Apply Suppress - Script 4 Apply Suppression from Suppress Table*/
DROP table IF EXISTS #SuppressEmail;
CREATE TABLE #SuppressEmail (EmailAddress varchar(200));

copy #SuppressEmail
from 's3://{s3-internal}/Sapphire/Sapphire_SuppressEmail.CSV'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter ','
ignoreheader as 1;

Delete from #SuppressEmail where EmailAddress ='';
/*Email suppress*/
UPDATE {sapphire-tbl-ctas1}
  set cInclude = 'S'
FROM {sapphire-tbl-ctas1}  A
INNER JOIN #SuppressEmail b
ON b.EmailAddress = A.EmailAddress
WHERE A.cInclude = 'Y';

drop table IF EXISTS #SuppressEmail;
/*
--Apply  Nixi File
--STEP2 : UPDATE Epd_haspostal, cInclude by matching reocrds to B2BNIXI.  71  for sapphire
*/
--uncomment after getting file location
UPDATE {sapphire-tbl-ctas1}
  SET Epd_haspostal  = CASE WHEN A.Epd_haspostal ='Y' THEN 'X' ELSE A.Epd_haspostal END,
      cInclude  = CASE WHEN A.cInclude ='Y' THEN 'X' ELSE A.cInclude END
from {sapphire-tbl-ctas1} A
Inner join EXCLUDE_B2BNIXI B on A.Individual_id = B.KeyField AND B.DatabaseID =71  ;

/*
Apply Suppress\Script 5 Apply Trap Emails

Dennis
Hi, There are 16 files in the Remove Traps folder on the W drive. Please remove the Valid Email file on Sapphire for any record that matches any of the email addresses on these files:

{USERVAR(PATH_IDMSFILES)}\Sapphire\BuildSupportFiles
*/

DROP table if Exists #Trapemails;
CREATE TABLE #TrapEmails (EmailAddress varchar(70));
copy #TrapEmails
from 's3://{s3-internal}{s3-key-buildsupportfiles}/SappphireTrapemails.TXT'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

CREATE TABLE #TrapEmails_dist (EmailAddress varchar(65));

INSERT INTO #TrapEmails_dist (EmailAddress)
Select DISTINCT UPPER(LEFT(LTRIM(RTRIM(EmailAddress)), 65)) AS EmailAddress
from #TrapEmails;

Delete from #TrapEmails_dist where EmailAddress ='';

/*--T  for TRAP*/

UPDATE {sapphire-tbl-ctas1}
set cInclude = 'T'
FROM {sapphire-tbl-ctas1} A
INNER JOIN #TrapEmails_dist b
ON b.EmailAddress = A.EmailAddress
WHERE A.cInclude = 'Y';

/* Apply Suppress\Script 6 Apply Bad Domain Suppressions*/

DROP table  IF EXISTS #Suppression;
CREATE TABLE #Suppression (domain varchar(200));
copy #Suppression
from 's3://{s3-internal}{s3-key-buildsupportfiles}/DomainList BAD from IG Combined NO ACTIVITY.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

DELETE FROM #Suppression WHERE Domain = '';
UPDATE #Suppression SET Domain = UPPER(LTRIM(RTRIM(Domain)));

/*--suppress*/
UPDATE {sapphire-tbl-ctas1}
  set cInclude = 'G'
FROM {sapphire-tbl-ctas1} A
INNER JOIN #Suppression b
ON A.EPD_Domain = b.Domain
WHERE a.cInclude in ('Y', 'M');

/* Apply Suppress - \Script 07 Apply_ParkSuppression  from File_if provided\Script 07 Apply ParkSuppressions from File

if client provided a new file with a date, do the following.  if not,  use the current files.

IF you get a new file,
copy the txt file to {USERVAR(PATH_IDMSFILES)}\Sapphire\BuildSupportFiles

*/
/*--ParkSuppression*/
drop table IF EXISTS #ParkSuppressionEmail;
CREATE TABLE #ParkSuppressionEmail (EmailAddress varchar(200) );
CREATE TABLE #ParkSuppressionDomain (cdomain varchar(200));

/*-domain file*/
copy #ParkSuppressionDomain
from 's3://{s3-internal}{s3-key-buildsupportfiles}/PARK Domain OMIT.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
ignoreheader as 1
IGNOREBLANKLINES;

/*-email file File 1.  change the file name*/
copy #ParkSuppressionEmail
from 's3://{s3-internal}{s3-key-buildsupportfiles}/ParkMarketZoneUniverseLessSapphireDISTINCT.txt'
iam_role '{iam}'
delimiter '\n'
ACCEPTINVCHARS FILLRECORD
ignoreheader as 1
IGNOREBLANKLINES; --load has issues(solved by adding /n as a delimeter)

Drop table if exists #ParkSuppressionEmail_dist ;
SELECT DISTINCT UPPER(LTRIM(RTRIM(EmailAddress))) as EmailAddress
INTO #ParkSuppressionEmail_dist
from #ParkSuppressionEmail;

UPDATE #ParkSuppressionDomain  SET cDomain = UPPER(LTRIM(RTRIM(cDomain)));

/*--Delete the header*/
DELETE from #ParkSuppressionEmail  where EmailAddress in ('EMAILADDRESS', 'CEMAIL','EMAIL')   or EmailAddress ='';
DELETE from #ParkSuppressionDomain  where  cDomain='';

/*email suppression -- */
Update {sapphire-tbl-ctas1}
  SET ParkSuppression  = 'Y'
From {sapphire-tbl-ctas1} A inner join  #ParkSuppressionEmail_dist  B  on A.EmailAddress =B.EmailAddress
where A.EmailAddress <>''  and (A.ParkSuppression  <> 'Y'  or A.ParkSuppression  is null) ;


/*domain suppression*/
Update {sapphire-tbl-ctas1}
  SET ParkSuppression  = 'Y'
From {sapphire-tbl-ctas1} A inner join  #ParkSuppressionDomain  B  on A.EPD_Domain =B.cdomain
where A.EPD_Domain <>''  and (A.ParkSuppression  <> 'Y'  or A.ParkSuppression  is null);

/*Set remaing records to 'N'*/
--uncomment after getting file location
Update {sapphire-tbl-ctas1}
  SET ParkSuppression  = 'N'
From {sapphire-tbl-ctas1}
where (ParkSuppression  is null);

Drop table IF EXISTS #ParkSuppressionEmail;
Drop table IF EXISTS #ParkSuppressionEmail_dist ;
Drop table IF EXISTS #ParkSuppressionDomain ;

/*--Email suppress*/
--uncomment after getting file location

UPDATE {sapphire-tbl-ctas1}
set cInclude = 'G'
FROM {sapphire-tbl-ctas1} a
INNER JOIN EXCLUDE_GST_SUPPRESSEMAILS b  ON b.EmailAddress = a.EmailAddress
WHERE a.cInclude = 'Y';
