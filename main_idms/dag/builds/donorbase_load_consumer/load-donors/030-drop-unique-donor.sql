DROP TABLE IF EXISTS donorbase_donors_unique;

CREATE TABLE donorbase_donors_unique
DISTKEY(Individual_ID)
AS
SELECT ListID, Individual_ID
      ,MAX(ID) ID
      ,MAX(AccountNo) AccountNo
      ,MAX(FirstName) FirstName
      ,MAX(LastName) LastName
      ,MAX(Title) Title
      ,MAX(AddressLine1) AddressLine1
      ,MAX(AddressLine2) AddressLine2
      ,MAX(City) City
      ,MAX(State) State
      ,MAX(ZIP) ZIP
      ,MAX(ZIP4) ZIP4
      ,MAX(Zipfull) Zipfull
      ,MAX(SCF) SCF
      ,MAX(Phone) Phone
      ,MAX(EmailAddress) EmailAddress
      ,MAX(Gender) Gender
      ,MAX(Company_ID) Company_ID
      ,MAX(IndividualMC) IndividualMC
      ,MAX(CompanyMC) CompanyMC
      ,MAX(Mailabilityscore) Mailabilityscore
      ,MAX(DeceasedFlag) DeceasedFlag,
      'N' AS UniqueFlag,
      'N' AS SuppressUnique  
 FROM donorbase_donors
WHERE LEFT(MailabilityScore, 1) <> '5'
  AND DeceasedFlag = 'N'
GROUP BY ListID, Individual_ID;

UPDATE donorbase_donors_unique
   SET UniqueFlag = 'Y',
       SuppressUnique = CASE WHEN ListID IN ({retain-unique-lol}) THEN 'Y' ELSE 'N' END
 WHERE Individual_ID IN 
  (
    SELECT Individual_ID
      FROM donorbase_donors_unique
     GROUP BY Individual_ID
     HAVING COUNT(DISTINCT ListID) = 1
  );

-- Delete Duplicates and Deceased from those lists where tblMasterLoL.iDropDuplicates = 1 AND iIsActive = 1 AND LK_ListType = 'C'
DELETE 
  FROM {maintable_name}
 WHERE DeceasedFlag = 'Y'
    OR (
        Individual_ID IN 
        (SELECT Individual_ID
           FROM donorbase_donors_unique
          WHERE ListID IN ({drop-unique-lol})
            AND UniqueFlag = 'Y')
        );

DELETE 
  FROM donorbase_donors_unique
 WHERE ListID IN ({drop-unique-lol})
   AND UniqueFlag = 'Y';

-- Update Main Table with Suppression Flag
UPDATE {maintable_name}
   SET SuppressUnique = 'Y' 
 WHERE (SuppressUnique IS NULL OR SuppressUnique <> 'Y')
   AND Individual_ID IN 
      (SELECT Individual_ID
         FROM donorbase_donors_unique
        WHERE SuppressUnique = 'Y');

-- Delete Donors that already exists in Consumer DB
 DELETE 
   FROM donorbase_donors_unique
  WHERE Individual_ID IN 
  (
    SELECT Individual_ID
      FROM {maintable_name}
  );

--Delete Donors with blank first or last name

 DELETE
   FROM donorbase_donors_unique
  WHERE (NVL(firstname,'')='' or NVL(lastname,'')='');

--Delete Donors with blank addressline1
DELETE
   FROM donorbase_donors_unique
  WHERE NVL(addressline1,'')='';