--Delete Donors with blank first or last name
 DELETE
   FROM nonprofit_donors_{dbid}
  WHERE (NVL(firstname,'')='' OR NVL(lastname,'')='');

--Delete Donors with blank addressline1
DELETE
   FROM nonprofit_donors_{dbid}
  WHERE NVL(addressline1,'')='';

DROP TABLE IF EXISTS nonprofit_donors_unique_{dbid};

CREATE TABLE nonprofit_donors_unique_{dbid}
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
      ,MAX(Zipfull) Zipfull
      ,MAX(ZIP) ZIP
      ,MAX(ZIP4) ZIP4
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
 FROM nonprofit_donors_{dbid}
WHERE LEFT(MailabilityScore, 1) <> '5'
  AND DeceasedFlag = 'N'
GROUP BY ListID, Individual_ID;

UPDATE nonprofit_donors_unique_{dbid}
   SET UniqueFlag = 'Y',
       SuppressUnique = CASE WHEN ListID IN ({retain-unique-lol}) THEN 'Y' ELSE 'N' END
 WHERE Individual_ID IN 
  (
    SELECT Individual_ID
      FROM nonprofit_donors_unique_{dbid}
     GROUP BY Individual_ID
    HAVING COUNT(DISTINCT ListID) = 1
  );

-- Delete Deceased and DropUniques from consumer universe
DELETE 
  FROM {maintable_name}
 WHERE DeceasedFlag = 'Y'
    OR (
        Individual_ID IN 
        (SELECT Individual_ID
           FROM nonprofit_donors_unique_{dbid}
          WHERE ListID IN ({drop-unique-lol})
            AND UniqueFlag = 'Y')
        );

DELETE 
  FROM nonprofit_donors_unique_{dbid}
 WHERE ListID IN ({drop-unique-lol})
   AND UniqueFlag = 'Y';

-- Update Main Table with Suppression Flag
UPDATE {maintable_name}
   SET SuppressUnique = 'Y' 
 WHERE (SuppressUnique IS NULL OR SuppressUnique <> 'Y')
   AND Individual_ID IN 
      (SELECT Individual_ID
         FROM nonprofit_donors_unique_{dbid}
        WHERE SuppressUnique = 'Y');

-- Delete Donors that already exists in Consumer DB
 DELETE 
   FROM nonprofit_donors_unique_{dbid}
  WHERE Individual_ID IN 
  (
    SELECT Individual_ID
      FROM {maintable_name}
  );

--Delete Donors from legacy Apogee DB CCPA
DELETE
  FROM nonprofit_donors_unique_{dbid}
 WHERE individual_id in
  (
    SELECT individual_id
      FROM exclude_tblsuppression_82
  );
