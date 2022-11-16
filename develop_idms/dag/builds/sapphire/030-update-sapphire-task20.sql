/*

IMPORTANT : Must use SPLIT INSERT because of  IQ Function. Reju m

*/


/*
We would like to add 2 new date fields for the May build by the postal go live date if possible:
1.	New Name date
a.	Set up and available in IDMS similar to ?last open click date?.
b.	Updated only at build time
i.	All individual match keys from the February build get the date 201302
ii.	All individual match keys on the May build that are not on the February build get the date 201305 (match keys from February would still have the date 201302)
iii.	All individual match keys on the August build that are not on the May build get 201308 (match keys from February would still have the date 201302, match keys from the May build would still have the date 201305)
iv.	Etc.
c.	Goal- allow new names (defined as different individual match key) to be selected by sales easily. This is a common request by customers.
2.	New Email date
a.	Set up and available in IDMS similar to ?last open click date?.
b.	Updated only at build time
i.	All email addresses from the February build (not just those with a Valid Email) get the date 201302
ii.	All email addresses from the May build that are not on the February build get the date 201305 (email addresses from February would still have the date 201302)
iii.	All email addresses from the August build that are not on the May build get 201308 (email addresses from February would still have the date 201302, email addresses from the May build would still have the date 201305)
iv.	Etc.
c.	Goal- allow new email addresses (defined as different individual match key) to be selected or excluded for sales easily.
i.	Note that every other update we are only going to send ?new? email addresses through BV. The May update will be the first one in this situation.

Please let me know if I should send this request differently or to DW orders. I will resend this when we near the build completion but wanted to give you advance notice too. Also let me know if you have any questions.


-- Reju Mathew
*/


/*
-- REPLACE   {TASK(1|Result.Parameter.Value|MainTableName)}   to new Build table
-- REPLACE   tblMain_{TASK(1|Result.Parameter.Value|PreviousBuildID)}_{TASK(1|Result.Parameter.Value|PreviousCBuild)}  to Previous Build table
--REPLACE  '{TASK(1|Result.Parameter.Value|Build)}'   (including quotes) with new Date.  for now 2014 build,  '201412'

--create,  if not found
*/
ALTER TABLE {sapphire-tbl-ctas1}
    add NewNameDate varchar(6);
ALTER TABLE {sapphire-tbl-ctas1}
    add NewEmailDate varchar(6);

Drop table IF EXISTS #NewNameDate;
CREATE TABLE #NewNameDate
(
    Individual_ID varchar(22),
    NewNameDate   Varchar(6)
)
DISTSTYLE key distkey ( individual_id )
sortkey (individual_id);

INSERT INTO #NewNameDate (Individual_ID, NewNameDate)
Select Individual_ID, MAX(NewNameDate) as NewNameDate
from {sapphire-pre-build}  /*  --old table*/
group by Individual_ID;
Delete
from #NewNameDate
where Individual_ID = ''
   or NewNameDate = '';

/*--Now,  uppdate NewNameDate*/
UPDATE {sapphire-tbl-ctas1}
SET NewNameDate = B.NewNameDate
FROM {sapphire-tbl-ctas1} A
         INNER JOIN #NewNameDate B
                    on A.Individual_ID = B.Individual_ID
where A.Individual_ID <> ''
  AND B.Individual_ID <> '';

drop table IF EXISTS #NewNameDate;

/*--Update remaining records with current month*/
UPDATE {sapphire-tbl-ctas1}
SET NewNameDate= '{build}'
FROM {sapphire-tbl-ctas1} A
where NewNameDate is NULL;

/*----------------------
-- do the same based on email.
*/

Drop table IF EXISTS #NewNameDate_email;
CREATE TABLE #NewNameDate_email
(
    EmailAddress varchar(100),
    NewNameDate  Varchar(6)
);
INSERT INTO #NewNameDate_email (EmailAddress, NewNameDate)
Select EmailAddress, MAX(NewNameDate) as NewNameDate
from {sapphire-pre-build}
where EmailAddress <>''
group by EmailAddress;

Delete
from #NewNameDate_Email
where EmailAddress = ''
   or NewNameDate = '';

/*--Now,  uppdate Newemail Date */
UPDATE {sapphire-tbl-ctas1}
SET NewEmailDate = B.NewNameDate
FROM {sapphire-tbl-ctas1} A
         INNER JOIN #NewNameDate_Email B
                    on A.EmailAddress = B.EmailAddress
where A.EmailAddress <> ''
  AND B.EmailAddress <> '';

/*--Update remaining records with current month*/
UPDATE {sapphire-tbl-ctas1}
SET NewEmailDate='{build}'
FROM {sapphire-tbl-ctas1} A
where A.EmailAddress <> ''
  and NewEmailDate is NULL;

--job title handled in CTAS1
--CombinedJobTitleAndFunction handled in CTAS1

/*
-- Apply ParkSuppression_From Previous build.
Get  the flag form previous update.
Overwrite with new file if any.  if no emails match in new file, it will keep the old date.

--first,  REPLACE ALL the New build  table with  most recent table
--the, Replace ALL Previous build  table with last build.

-- Reju Mathew
*/

/*-Parksuppression created with lastclickopen */

/*--Create Suppression flag. logic by jayesh.  Reju*/
--HANDLED IN CTAS 1
-- ALTER TABLE {sapphire-tbl-ctas1}
--     ADD Email_Suppression_Flag CHAR(1);
-- ALTER TABLE {sapphire-tbl-ctas1}
--     ADD DM_TM_Suppression_Flag CHAR(1);
--
-- UPDATE {sapphire-tbl-ctas1}
-- SET Email_Suppression_Flag = CASE WHEN Individual_ID in
--                                        (Select Individual_ID from {sapphire-pre-build} where Email_Suppression_Flag = 'Y') THEN 'Y'
-- when Email_Suppression_Flag is null THEN 'N' END,
-- DM_TM_Suppression_Flag = CASE WHEN Individual_ID in (Select  Individual_ID from  {sapphire-pre-build}  where DM_TM_Suppression_Flag  = 'Y') THEN 'Y'
-- WHEN DM_TM_Suppression_Flag is null THEN 'N' END
-- FROM {sapphire-tbl-ctas1};

--HasNameAndTitle handled in CTAS1

ALTER TABLE {sapphire-tbl-ctas1} ADD LiveRampMatchBack_Email CHAR (1);
ALTER TABLE {sapphire-tbl-ctas1} ADD LiveRampMatchBack_Postal CHAR (1);

DROP TABLE IF EXISTS {sapphire-tbl-ctas1}_15021_ToBeDropped;

select *
into {sapphire-tbl-ctas1}_15021_ToBeDropped
from {sapphire-tbl-ctas1}
where ListID = 15021
  and Individual_ID in (select distinct Individual_id from {sapphire-tbl-ctas1} where ListID = 8537);

DELETE
FROM {sapphire-tbl-ctas1}
WHERE ID IN (SELECT ID FROM {sapphire-tbl-ctas1}_15021_ToBeDropped);

/*
Character Removal from Name Field  sharepoint#405
Incident# 639589 [Active] PROJECT: Create a SP in IQ to clean junk chars

--Reju Mathew 2017.09.17
*/
-- usp_removeJunkCharacters to be handled in SQl server
-- Execute usp_RemoveJunkCharacters '{TASK(1|Result.Parameter.Value|MainTableName)}', 'FirstName';
-- Execute usp_RemoveJunkCharacters '{TASK(1|Result.Parameter.Value|MainTableName)}', 'LastName';
-- Execute usp_RemoveJunkCharacters '{TASK(1|Result.Parameter.Value|MainTableName)}', 'Fullname';

CREATE TABLE #PhoneFlag
(
    PhoneWithFlag varchar(20)
);
Truncate table #PhoneFlag;

COPY #PhoneFlag
    FROM 's3://{s3-internal}{s3-key-buildsupportfiles}/wireless.txt'
    iam_role '{iam}'
    ACCEPTINVCHARS FILLRECORD
    delimiter '|'
    ignoreheader as 1;

CREATE TABLE #PhoneFlag_Dist (PhoneCode VARCHAR(7));
INSERT INTO #PhoneFlag_Dist (PhoneCode)  SELECT DISTINCT LEFT(PhoneWithFlag,7)  from #PhoneFlag ;
--phone cell flag added in CTAS
UPDATE {sapphire-tbl-ctas1}
   SET Phone_CellFlag ='Y'
from {sapphire-tbl-ctas1} A
inner join #PhoneFlag_Dist B on Left(A.Phone,7) =B.PhoneCode
where (A.Phone  is not null and A.Phone <>'');
DROP TABLE IF EXISTS #PhoneFlag;
DROP TABLE IF EXISTS #PhoneFlag_Dist;

/*
--RE: Incident# 643346 [Logged] Sapphire Phone Quality Flag needed
RejuM 2017.11.29  Added to get it from previous build
*/


--Phone_quality_flag added in CTAS
UPDATE {sapphire-tbl-ctas1}
     SET Phone_quality_flag  = B.Phone_quality_flag
FROM {sapphire-tbl-ctas1} A
INNER JOIN {sapphire-pre-build}  B
on A.Individual_id = B.Individual_id and A.phone =B.Phone
where A.Individual_id <>'' and A.phone <>'';