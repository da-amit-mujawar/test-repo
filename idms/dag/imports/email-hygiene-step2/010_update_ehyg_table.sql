-- Add columns from Engaged table
ALTER TABLE dbo.ehyg_{dbid} ADD Engaged_Flag char(1);
ALTER TABLE dbo.ehyg_{dbid} ADD Engaged_Open_Date varchar(8);
ALTER TABLE dbo.ehyg_{dbid} ADD Engaged_Click_Date varchar(8);
--------------------
UPDATE
    ehyg_{dbid}
SET
    Engaged_Flag = 'N',
    Engaged_Open_Date = ' ',
    Engaged_Click_Date = ' '
WHERE
    email > ' ';
--------------------
UPDATE
    ehyg_{dbid}
SET
    Engaged_Flag = 'Y',
    Engaged_Open_Date = open_date,
    Engaged_Click_Date = click_date
FROM
    exclude_engaged_12_mo 
WHERE
    ehyg_{dbid}.email = exclude_engaged_12_mo.emailaddress;
--======================================================================================

DROP TABLE IF EXISTS ehyg_{dbid}_EO_Returns;
CREATE TABLE ehyg_{dbid}_EO_Returns
(
    email VARCHAR(100),
    ValidationStatusID varchar(10),
	ValidationStatus VARCHAR(50),
	EmailDomainGroupID varchar(10),
	EmailDomainGroup VARCHAR(50)
);
-- Load S3 file into redshift table 
COPY ehyg_{dbid}_EO_Returns
FROM 's3://{bucket_name}/{file_key}'
IAM_ROLE '{iam}'
CSV QUOTE AS '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 IGNOREBLANKLINES;
-- update ehyg table using returned oversight file
ALTER TABLE ehyg_{dbid} ADD EO_Validation_StatusID varchar(10);
ALTER TABLE ehyg_{dbid} ADD EO_Validation_Status varchar(50);
ALTER TABLE ehyg_{dbid} ADD EO_EmailDomainGroupID varchar(10);
ALTER TABLE ehyg_{dbid} ADD EO_EmailDomainGroup varchar(50);

UPDATE ehyg_{dbid}
	SET	EO_Validation_StatusID =' ',
		EO_Validation_Status =' ',
		EO_EmailDomainGroupID =' ',
		EO_EmailDomainGroup =' ';
UPDATE ehyg_{dbid}
	SET	EO_Validation_StatusID = ValidationStatusID,
		EO_Validation_Status = ValidationStatus,
		EO_EmailDomainGroupID = EmailDomainGroupID,
		EO_EmailDomainGroup = EmailDomainGroup
	from ehyg_{dbid}_EO_Returns where ehyg_{dbid}_EO_Returns.email = ehyg_{dbid}.email;

--======================================================================================
-- Add ehyg code to table using Case When
ALTER TABLE ehyg_{dbid} ADD Ehygiene_Code char(1);

UPDATE ehyg_{dbid} SET Ehygiene_Code = ' ';
UPDATE ehyg_{dbid}
SET
    Ehygiene_Code = CASE
        WHEN EO_Validation_Status = 'Role'
        OR EO_Validation_Status = 'Bot'
        OR EO_Validation_Status = 'MalfORmed'
        OR EO_Validation_Status = 'Disposable Email'
        OR EO_Validation_Status = 'Seed Account'
        OR EO_Validation_Status = 'Undeliverable'
        OR EO_Validation_Status = 'SpamTrap' THEN '0'
        WHEN Engaged_Flag = 'Y' THEN '1'
        WHEN EO_Validation_Status = 'Verified' THEN '2'
        WHEN EO_Validation_Status = 'Catch All' THEN '3'
        WHEN EO_Validation_Status = 'Unknown'
        OR EO_Validation_Status = 'Complainer' THEN '4'
        ELSE '0'
    END
WHERE
    email > ' ';
