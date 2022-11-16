/*
Incident #	: 698821
Summary	: SAPPHIRE Standardizing Preferred Records
Please remove Non-North American records from Sapphire
Requested for August 1 or asap  Active June Build & future builds.

Goal: Records in Sapphire are North America only
 US records will have postal + email.
 Canada will be postal only.

1. Identify Valid Postal Addresses for US & Canada
a. Logic to CB for approval
2. DROP ALL records without Valid Postal Addresses in US or Canada
a. Create Audit Report with Before & After counts
3. US records may have an email address, if not in EU domain suffixes attached.
a. Blank out email if it matches EU domain suffixes attached, no backup of blanked emails required
b. Note: Records with a US postal address and domain suffix not matching EU list may remain
c. Add counts to Audit Report
4. Canada records may NOT have an email address.
a. Blank out email, no backup of blanked emails required
b. Add counts to Audit Report


Current Sapphire Canada Email Logic for reference:
Canada Emails are NOT removed at the time of List conversion. During Build process, Canada Emails are archived to an unpublished field.
Condition: Country = Canada or email domains from Canada.
Note: Lists loaded from other B2B DBs already in IDMS, would have Canada emails already removed.

Caroline Burch


Aprroved Logic for suppression
Remove any records where NOT CountryFlag in (‘A’,’B’,’C’)
Remove any emails where CountryFlag in (‘A’,’B’) AND EUTopLevelDomain
Remove any emails where CountryFlag = ‘C’

Reju Mathew 2018.07.24
*/
--Create a Table to hold the suppress IDs
DROP TABLE IF EXISTS #DeleteNonUSCanadaRecords;
CREATE TABLE #DeleteNonUSCanadaRecords (ID int, cFlag Varchar (100), cAction Varchar(20));
--Create a Table to hold the counts
DROP TABLE IF EXISTS  Sapphire_test_PreferredRecordCount_ToBeDropped;
CREATE TABLE  Sapphire_test_PreferredRecordCount_ToBeDropped (
        ID int identity,
        CountryFlag Varchar(1),
        Country Varchar(30) ,
        RecordCount int,
        HasEmail int,
        EUTopLevelDomain int,
        cFlag Varchar (100));

--Count Before suppress records
INSERT INTO  Sapphire_test_PreferredRecordCount_ToBeDropped (CountryFlag,Country,RecordCount,HasEmail, EUTopLevelDomain,cFlag)
Select CountryFlag,
        CASE
            WHEN CountryFlag = ('A') THEN 'USA - States'
            WHEN CountryFlag = 'B' THEN 'USA - APO/FPO/Terr'
            WHEN CountryFlag = 'C' THEN 'CANADA'
        ELSE 'Non-USA/CANADA' END as country,
        Count(*) AS RecordCount,
        SUM(CASE WHEN EmailAddress <> '' THEN 1 else 0 end) as HasEmail,
        SUM(CASE WHEN TopLevelDomain  in ('.AD','.AL','.AM','.AT','.AZ','.BA','.BE','.BG','.BY','.CAT','.CH','.CY','.CZ','.DE','.DK','.EE','.ES','.EU','.FI',
                 '.FR','.GE','.GG','.GL','.GR','.HR','.HU','.IE','.IM','.IS','.IT','.JE','.KZ','.LI','.LT','.LU','.LV','.MC','.MD',
                '.ME','.MK','.MT','.NL','.NO','.PL','.PT','.RO','.RS','.RU','.SE','.SI','.SK','.SM','.SU','.TR','.UA','.UK','.VA')
                        THEN 1 ELSE 0 END) as EUTopLevelDomain,
        'Before Suppression' as cFlag
from {sapphire-tbl-ctas1}
Where Listid >0
Group by CountryFlag
order by 1;

/*
-- SUPPRESS 1  - Remove any records where NOT CountryFlag in ('A','B','C') .  Suppress Foreign  postal records.
*/
INSERT INTO #DeleteNonUSCanadaRecords (ID, cFlag, cAction )
  SELECT ID, 'Suppress Foreign postal records' as cFlag, 'SUPPRESS'
--Select Count(*)
from {sapphire-tbl-ctas1} where CountryFlag not in ('A', 'B','C');

/*
-- SUPPRESS 2  - Remove any emails where CountryFlag in ('A','B') AND EUTopLevelDomain
*/
INSERT INTO #DeleteNonUSCanadaRecords (ID, cFlag, cAction )
  SELECT Distinct ID, 'Remove US -  ''EU domains'' Emails ' as cFlag, 'REMOVEEMAIL' as cAction
--Select count(*)
FROM {sapphire-tbl-ctas1} A
Where CountryFlag in ('A','B')
and TopLevelDomain  in ('.AD','.AL','.AM','.AT','.AZ','.BA','.BE','.BG','.BY','.CAT','.CH','.CY','.CZ','.DE','.DK','.EE','.ES','.EU','.FI',
                 '.FR','.GE','.GG','.GL','.GR','.HR','.HU','.IE','.IM','.IS','.IT','.JE','.KZ','.LI','.LT','.LU','.LV','.MC','.MD',
                '.ME','.MK','.MT','.NL','.NO','.PL','.PT','.RO','.RS','.RU','.SE','.SI','.SK','.SM','.SU','.TR','.UA','.UK','.VA');

/*
-- SUPPRESS 3  - Remove any emails where CountryFlag = 'C'
--this is addtion to dropping all .ca extensions in CALCUATIONs -1
*/
INSERT INTO #DeleteNonUSCanadaRecords (ID, cFlag,cAction )
  SELECT Distinct ID, 'Remove CANADA Emails' as cFlag,'REMOVEEMAIL' as cAction
--Select count(*)
FROM {sapphire-tbl-ctas1} A
Where CountryFlag in ('C')
and EmailAddress <>'';

/*
Apply the fixes on data
*/
--final count, to be dropped.
Select cFlag, cAction, count(*)
from #DeleteNonUSCanadaRecords
group by cFlag,cAction;

--1.  SET List id =-1  on Suppress records
Update {sapphire-tbl-ctas1}
  SET LISTID = a.LISTID * -1
--Select 'Suppress Count:', count(*)
from {sapphire-tbl-ctas1} A
inner join #DeleteNonUSCanadaRecords  B  on a.id =B.ID   AND B.CAction ='SUPPRESS'
Where A.ListID >0;

--2.  REMOVE EMAIL.  Take a Backup  first
ALTER TABLE {sapphire-tbl-ctas1} ADD RemovedData_Raw02 Varchar(80);
Update {sapphire-tbl-ctas1}
  SET RemovedData_Raw02 = LEFT(a.EmailAddress,80),
      EmailAddress = '',
      cInclude ='S' ,
      EmailAddress_MD5='',
      EncryptedEmail=''
--Select 'REMOVE EMAIL Count:', count(*)
from {sapphire-tbl-ctas1} A
inner join #DeleteNonUSCanadaRecords  B  on a.id =B.ID   AND B.CAction ='REMOVEEMAIL';

--Count after the  suppress records
INSERT INTO  Sapphire_test_PreferredRecordCount_ToBeDropped (CountryFlag,Country,RecordCount,HasEmail, EUTopLevelDomain,cFlag)
Select CountryFlag,
        CASE
            WHEN CountryFlag = ('A') THEN 'USA - States'
            WHEN CountryFlag = 'B' THEN 'USA - APO/FPO/Terr'
            WHEN CountryFlag = 'C' THEN 'CANADA'
            ELSE 'Non-USA/CANADA'
            END as Country,
        Count(*) AS RecordCount,
        SUM(CASE WHEN EmailAddress <> '' THEN 1 else 0 end) as HasEmail,
        SUM(CASE WHEN (EmailAddress <> '' AND TopLevelDomain  in ('.AD','.AL','.AM','.AT','.AZ','.BA','.BE','.BG','.BY','.CAT','.CH','.CY','.CZ','.DE','.DK','.EE','.ES','.EU','.FI',
                 '.FR','.GE','.GG','.GL','.GR','.HR','.HU','.IE','.IM','.IS','.IT','.JE','.KZ','.LI','.LT','.LU','.LV','.MC','.MD',
                '.ME','.MK','.MT','.NL','.NO','.PL','.PT','.RO','.RS','.RU','.SE','.SI','.SK','.SM','.SU','.TR','.UA','.UK','.VA'))
                        THEN 1 ELSE 0 END) as EUTopLevelDomain,
        'After Suppression' as cFlag
from {sapphire-tbl-ctas1}
Where Listid >0
Group by CountryFlag
order by 1;
