DROP TABLE IF EXISTS SapphireBuildCounts_ToBeDropped;

--create few buckets to hold the counts.
CREATE TABLE SapphireBuildCounts_ToBeDropped (ID int identity, Col1  Varchar(100),Col2  Varchar(100), Col3  Varchar(100), Col4  Varchar(100), Col5  Varchar(100) );

--1. Count email Flags
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Counts By EmailFlags';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'EmailFlags', 'Count(*)' , 'COUNT(Distinct EmailAddress)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
        Select cInclude, CAST(count(*) AS Varchar(12) ) , CAST( COUNT(distinct A.EmailAddress) AS Varchar(12))
        from {sapphire-main-ctas} A
        where EmailAddress <>''
        group by cInclude
        order by cInclude;

--2. Count Empty Emails
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Check Email Flags of Empty Emails';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'EmailFlags', 'Count(*)' , 'COUNT(Distinct individual_ID)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
        Select cInclude, CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12))
        from {sapphire-main-ctas} A
        where EmailAddress =''
        group by cInclude
        order by cInclude;

--3. Count Canada Emails
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Check Canada Email Flags. No Y records allowed for Canada';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'EmailFlags', 'Count(*)' , 'COUNT(Distinct individual_ID)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
    SELECT cInclude,CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12))
    FROM {sapphire-main-ctas} A
    where EmailAddress <>''
    and   (EmailAddress LIKE '%.CA' OR (EPD_Domain LIKE '%.CA' OR COUNTRYNAME='CANADA'))
    OR (COUNTRYNAME<>'CANADA' and ZipFull<>'' AND REGEXP_COUNT(LEFT(ZipFull, 5), '^[0-9]+$') = 0)
    group by cInclude;

--4. Count  Total Records
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Count  Total Records';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'Count(*)', 'COUNT(Distinct individual_ID)', 'COUNT(Distinct EmailAddress)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
    SELECT CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12)), CAST(COUNT(distinct A.EmailAddress) as VARCHAR(12))
    FROM {sapphire-main-ctas} A ;

--5. Count  Good Emails
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Count  Good Emails (Y) Records';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'Count(*)', 'COUNT(Distinct individual_ID)', 'COUNT(Distinct EmailAddress)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
    SELECT CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12)), CAST(COUNT(distinct A.EmailAddress) as VARCHAR(12))
    FROM {sapphire-main-ctas}  A
    Where cInclude ='Y';

--6. Count  by CountryFlag
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Count  by CountryFlag';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'CountryFlag', 'Count(*)', 'COUNT(Distinct individual_ID)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
    SELECT CountryFlag, CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12))
    FROM {sapphire-main-ctas} A
    Group by CountryFlag
    ORDER BY CountryFlag;

--7. Count by new Names
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Count  by NewNameDate';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'NewNameDate', 'Count(*)', 'COUNT(Distinct individual_ID)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
    SELECT NewNameDate, CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12))
    FROM {sapphire-main-ctas} A
    Group by NewNameDate
    ORDER By NewNameDate;

--8. Count by New EmailDates -(Ys only)
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1)  Select 'Count by New EmailDates -(Ys only)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)  Select 'NewEmailDate', 'Count(*)', 'COUNT(Distinct individual_ID)';
INSERT INTO SapphireBuildCounts_ToBeDropped (Col1, Col2,Col3)
    SELECT NewEmailDate, CAST (count(*) As Varchar(12)), CAST (COUNT(Distinct individual_ID) As Varchar(12))
    FROM {sapphire-main-ctas} A
    where cinclude ='Y'
    Group by NewEmailDate
    ORDER By NewEmailDate;