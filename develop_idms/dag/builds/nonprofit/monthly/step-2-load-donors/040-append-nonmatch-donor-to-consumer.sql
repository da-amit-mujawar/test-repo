-- Delete Non-Matches if they were loaded from previous run
DELETE FROM {maintable_name} WHERE ListID <> {consumer-listid};

INSERT INTO {maintable_name}
(
    ListID,
    Individual_ID,
    AccountNo,
    Firstname,
    Lastname,
    AddressLine1,
    City,
    State,
    Zip,
    Zip4,
    Zip9,
    SCF,
    Areacode,
    Phonenumber,
    Emailaddress,
    Gender,
    Company_ID,
    Individual_MC,
    Company_MC,
    Mailconfidence,
    Mailscore,
    DeceasedFlag,
    UniqueFlag,
    SuppressUnique
)
SELECT ListID
      ,LEFT(Individual_ID, 25) Individual_ID
      ,LEFT(AccountNo, 95) AccountNo
      ,LEFT(FirstName, 15) FirstName
      ,LEFT(LastName, 20) LastName
      -- ,LEFT(Title) Title Needs to decode
      ,LEFT(AddressLine1, 45) AddressLine1
      ,LEFT(City, 30) City
      ,LEFT(State, 2) State
      ,LEFT(ZIP, 5) ZIP
      ,LEFT(ZIP4, 4) ZIP4
      ,Zipfull
      ,LEFT(SCF, 3) SCF
      ,LEFT(TRIM(Phone), 3) AreaCode -- Split
      ,RIGHT(TRIM(Phone), 7) Phone -- Split
      ,LEFT(EmailAddress, 40) EmailAddress
      ,Gender
      ,LEFT(Company_ID, 25) Company_ID
      ,LEFT(IndividualMC, 25) IndividualMC
      ,LEFT(CompanyMC, 25) CompanyMC
      ,Mailabilityscore Mailconfidence-- Split
      ,LEFT(Mailabilityscore, 1) Mailscore -- Split
      ,DeceasedFlag
      ,UniqueFlag
      ,SuppressUnique  
FROM nonprofit_donors_unique_{dbid};

-- Delete Suppressions carried forward from On-prem IQ Server.
DELETE 
  FROM {maintable_name}
 WHERE individual_id in (SELECT individual_id from exclude_tblsuppression_82);

-- Delete CCPA Suppressions
DELETE
FROM {maintable_name}
WHERE (company_mc IN (SELECT DISTINCT company_mc FROM exclude_ccpa_suppression ))
      or
      (individual_mc IN (SELECT DISTINCT individual_mc FROM exclude_ccpa_suppression )) ;


