/*
--Nicole
RE: Incident# 545473 [Assigned] Liveramp Flag added Into IDMS

Create a flag  in sapphire .  LiveRampMatchBack_Email


WE EXPECT  THESE FILES  with Same file name and layout.

{USERVAR(PATH_IDMSFILES)}\Sapphire
LiveRampMatchBack_Email.txt    (for LiveRampMatchBack_Email )

Layout
Individual_Mc|Flag


--Reju Mathew
--2016.01.13


*/

/*  REPLACE   {TASK(1|Result.Parameter.Value|MainTableName)}  with new build */


/*
Create, if needed
ALTER TABLE {TASK(1|Result.Parameter.Value|MainTableName)} ADD LiveRampMatchBack_Email CHAR (1);
*/

/* load the Email File file. */

DROP TABLE IF EXISTS LiveRampMatchBack_ToBeDropped;
CREATE TABLE LiveRampMatchBack_ToBeDropped (
    Individual_ID varchar(30), cFlag VARCHAR(10))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

copy LiveRampMatchBack_ToBeDropped
from 's3://{s3-internal}/Sapphire/LiveRampMatchBack_Email.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

/* Clean the data */
DELETE FROM LiveRampMatchBack_ToBeDropped WHERE (ltrim(RTrim(cFlag)) = '' OR cFlag is null);
UPDATE LiveRampMatchBack_ToBeDropped SET cFlag = UPPER(ltrim(RTrim(cFlag))) ;

DROP TABLE IF EXISTS LiveRampMatchBack_Dist_ToBeDropped;
/*--Get the distinct records */

SELECT DISTINCT Individual_ID,  (CASE WHEN LEFT(LTRIM(RTRIM(cFlag)),1) ='T' THEN 'Y' ELSE 'N' End) as cFlag
INTO LiveRampMatchBack_Dist_ToBeDropped
FROM LiveRampMatchBack_ToBeDropped;

/*--Update the LiveRampMatchBack_Email  Flag .  Email suppress  */

UPDATE {sapphire-tbl-ctas1}
  set LiveRampMatchBack_Email = B.cFlag
FROM {sapphire-tbl-ctas1} A
INNER JOIN LiveRampMatchBack_Dist_ToBeDropped b
    ON b.Individual_ID = a.Individual_ID;

/*--Drop the staging tables.*/
DROP TABLE  IF EXISTS LiveRampMatchBack_ToBeDropped;
DROP TABLE IF EXISTS LiveRampMatchBack_Dist_ToBeDropped;
/*
load the Email File file.
*/
CREATE TABLE LiveRampMatchBack_ToBeDropped (
    Individual_ID varchar(100), cFlag VARCHAR(100))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

copy LiveRampMatchBack_ToBeDropped
from 's3://{s3-internal}/Sapphire/LiveRampMatchBack_Postal.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;

/*--Clean the data*/
DELETE FROM LiveRampMatchBack_ToBeDropped WHERE (ltrim(RTrim(cFlag)) = '' OR cFlag is null);commit;
UPDATE LiveRampMatchBack_ToBeDropped SET cFlag = UPPER(ltrim(RTrim(cFlag))) ; Commit;

DROP TABLE IF EXISTS LiveRampMatchBack_Dist_ToBeDropped;

/*--Get the distinct records*/
SELECT DISTINCT Individual_ID,  (CASE WHEN LEFT(LTRIM(RTRIM(cFlag)),1) ='T' THEN 'Y' ELSE 'N' End) as cFlag
INTO LiveRampMatchBack_Dist_ToBeDropped
FROM LiveRampMatchBack_ToBeDropped;

UPDATE {sapphire-tbl-ctas1}
  set LiveRampMatchBack_Postal = B.cFlag
FROM {sapphire-tbl-ctas1} A
INNER JOIN LiveRampMatchBack_Dist_ToBeDropped b
    ON b.Individual_ID = a.Individual_ID;

--Drop the staging tables.
DROP TABLE  IF EXISTS LiveRampMatchBack_ToBeDropped;
DROP TABLE IF EXISTS LiveRampMatchBack_Dist_ToBeDropped;
