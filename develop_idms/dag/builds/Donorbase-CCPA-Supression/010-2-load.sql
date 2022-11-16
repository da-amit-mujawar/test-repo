COPY Adhoc_CCPA_suppression 
(
      TableID
      ,ListID
      ,AccountNo
      ,FullName
      ,FirstName
      ,LastName
      ,Title
      ,Company
      ,AddressLine1
      ,AddressLine2
      ,City
      ,State
      ,ZIP
      ,ZIPFULL
      ,ZIP4
      ,SCF
      ,Phone
      ,EmailAddress
      ,Gender
      ,ListType
      ,ProductCode
      ,BatchDate
      ,Individual_ID
      ,Company_ID
      ,DropFlag
      ,PermissionType
      ,ListCategory01
      ,ListCategory02
      ,ListCategory03
      ,ListCategory04
      ,ListCategory05
      ,List_VolunteerInd
      ,Detail_DonationDollar
      ,Detail_DonationDate
      ,Detail_PaymentMethod
      ,Detail_DonationChannel
      ,Deceased
      ,DoNotMail
      ,DoNotCall
      ,RawField01
      ,RawField02
      ,RawField03
      ,RawField04
      ,RawField05
      ,RawField06
      ,RawField07
      ,RawField08
      ,RawField09
      ,RawField10
      ,RawField11
      ,RawField12
      ,RawField13
      ,RawField14
      ,RawField15
      ,IndividualMC
      ,CompanyMC
      ,MailDate
      ,SourceCode1
      ,SourceCode2
      ,SourceCode3
      ,SourceCode4
      ,SourceCode5
      ,Mailabilityscore
      ,DeceasedFlag
)
FROM 's3://{s3-donorbase-silver}/etl/build-output/DW_Final_1438_23710_20321/'
IAM_ROLE '{iam}'
DELIMITER '~'
GZIP
TRUNCATECOLUMNS
ACCEPTINVCHARS
TRIMBLANKS
MAXERROR 10000;


