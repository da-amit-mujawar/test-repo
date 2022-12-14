DROP TABLE IF EXISTS {tablename3} ; 

CREATE TABLE {tablename3}
(
ID bigint IDENTITY UNIQUE encode az64,
MMDBKey varchar (12) default ''  encode zstd,
IUSAKey varchar (19) default '' encode zstd,
DT_ABINumber varchar(70) default '' encode zstd, 
DT_FirstName varchar(150)  default '' encode zstd, 
DT_MiddleName varchar(150) default '' encode zstd, 
DT_LastName varchar(150) default '' encode zstd, 
DT_NameSuffix varchar(150) default '' encode zstd, 
DT_Address varchar(70) default '' encode zstd, 
DT_City varchar(28)  default '' encode zstd, 
DT_State varchar(20)  default '' encode zstd, 
DT_ZipCode varchar(15) default '' encode zstd, 
DT_Zip4 varchar(15) default '' encode zstd, 
DT_CarrierRouteCode varchar(70) default '' encode bytedict, 
DT_Company varchar(70) default '' encode zstd, 
DT_CorporateIndividual char(1) default '' encode zstd, 
DT_DateAdded varchar(8) default '' encode zstd, 
DT_DateAdded_Day varchar(32) default '' encode zstd,  
DT_DateAdded_Month varchar(15) default '' encode zstd,  
DT_DateAdded_Year varchar(30) default '' encode zstd, 
DT_DateLastReceived varchar(8) default '' encode zstd, 
DT_DateLastReceived_Day varchar(30) default '' encode zstd,  
DT_DateLastReceived_Month varchar(15) default '' encode zstd,  
DT_DateLastReceived_Year varchar(30) default '' encode zstd, 
DT_DeliverabilityScore varchar(20) default '' encode zstd,  
DT_DeliveryPointBarCode varchar(30) default '' encode zstd, 
DT_ExpirationDate varchar(8) default '' encode zstd,  
DT_ExpirationDate_Day varchar(8) default '' encode zstd,  
DT_ExpirationDate_Month varchar(2) default '' encode zstd,  
DT_ExpirationDate_Year varchar(30) default '' encode zstd, 
DT_Filing_Date varchar(8) default '' encode zstd, 
DT_Filing_Day varchar(8) default '' encode zstd, 
DT_Filing_Month varchar(2) default '' encode zstd, 
DT_Filing_Year varchar(20) default '' encode zstd,  
DT_FilingState varchar(20) default '' encode zstd,  
DT_FilingStatus char(2) default '' encode zstd, 
DT_FilingTypeCode varchar(2) default '' encode zstd,  
DT_OriginalFilingID varchar(70) default '' encode zstd, 
DT_PartyType char(1) default '' encode zstd, 
DT_SourceFilingID varchar(50) default '' encode zstd,
--new fields for UCC combined
DT_USPSRecordType varchar (2) default '' encode zstd, 
DT_MatchType char(1) encode zstd,  --match_code_LRFS 
DT_MatchScore varchar (10) default '' encode zstd, 
DT_CollateralType_Comb varchar(50) default '' encode zstd,
DT_CollateralType_Multi varchar(50) default '' encode zstd, --multi select field 
DT_CollateralType varchar(3) default '' encode zstd, 
DT_CollateralType2 varchar (3) default '' encode zstd,
DT_CollateralType3 varchar (3) default '' encode zstd,
DT_CollateralType4 varchar (3) default '' encode zstd,
DT_CollateralType5 varchar (3) default '' encode zstd,
DT_CollateralType6 varchar (3) default '' encode zstd,
DT_CollateralType7 varchar (3) default '' encode zstd,
DT_CollateralType8 varchar (3) default '' encode zstd,
DT_CollateralType9 varchar (3) default '' encode zstd,
DT_CollateralType10 varchar (3) default '' encode zstd,
DT_StateCode VARCHAR (2) default'' encode zstd,
DT_CountyCode VARCHAR(10) default '' encode bytedict,
DT_CountyCode_Description VARCHAR(14) default '' encode zstd,

--Securred party fields
SP_ABINumber  varchar (90) default '' encode zstd,
SP_LocationName varchar(70) default '' encode zstd, 
SP_Address varchar(70) default '' encode zstd, 
SP_City varchar(30) default '' encode zstd, 
SP_State varchar(3) default '' encode zstd,  
SP_Zip varchar(20) default '' encode zstd, 
SP_ZIP4 varchar(20) default '' encode zstd, 

--IG Exec fields
IGE_FullName varchar(32) default '' encode zstd, 
IGE_TitleCode  varchar(3) default '' encode zstd,
IGE_TitleCode_Back Varchar(30) default '' encode zstd, 
IGE_VendorEthnicity varchar (30) default '' encode zstd,  
IGE_ContactID varchar (12) default '' encode zstd,

--IG fields
--TTLDS_LRFS varchar(18) encode zstd, 
IG_Company varchar (150) encode zstd,
IG_Mailing_Address varchar(150) default '' encode zstd, 
IG_Mailing_City varchar(16) default '' encode zstd, 
IG_Mailing_State varchar(5) default '' encode zstd, 
IG_Mailing_StateCode varchar(5) default '' encode zstd,  
IG_Mailing_Zip varchar(5) default '' encode zstd,
IG_Mailing_ZipFour varchar(10) default '' encode zstd,
IG_Mailing_CarrierRouteCode varchar(10) default '' encode zstd, 
IG_Location_Address varchar(150) default '' encode zstd, 
IG_Location_City varchar(16) default '' encode zstd, 
IG_Location_State varchar(150) default '' encode zstd, 
IG_Location_Zip varchar(10) default '' encode zstd, 
IG_Location_ZipFour varchar(70) default '' encode zstd, 
IG_Location_CarrierRouteCode varchar(10) default '' encode zstd, 
IG_CountyCode varchar(10) default '' encode zstd, 
IG_CensusTract varchar(150) default '' encode zstd, 
IG_CensusBlock char(10) default '' encode zstd, 
fLATITUDE varchar(18) encode zstd, --BigInt encode zstd,
fLONGITUDE varchar(18) encode zstd, -- BigInt encode zstd,
IG_MatchCode char(10) default '' encode zstd, 
IG_PhoneNumber varchar(12) default '' encode zstd, 
IG_FaxPhone varchar(12) default '' encode zstd, 
IG_PrimarySIC6 varchar(150) default '' encode zstd, 
IG_SICCode1 varchar(150) default '' encode zstd, 
IG_SICCode2 varchar(150) default '' encode zstd, 
IG_SICCode3 varchar(150) default '' encode zstd, 
IG_SICCode4 varchar(150) default '' encode zstd, 
IG_NAICSCode varchar(8) default '' encode zstd, 
IG_EmployeeSizeCode2 char(10) default '' encode zstd, 
IG_NumberEmployees varchar(10) encode bytedict, 
IG_OutputVolumeCode2 char(10) default '' encode zstd, 
IG_OutputVolume2 varchar(8) encode zstd, --8  int to varchar
IG_SubsidairyABINumber varchar(150) default '' encode zstd, 
IG_ParentABINumber varchar(150) default '' encode zstd, 
IG_BusinessStatusCode char(10) default '' encode zstd, 
IG_YearFirstAppeared varchar(10) default '' encode zstd, 
IG_CreditAlphaScore varchar(20) default '' encode zstd, 
IG_CreditNumericScore varchar(10) default '' encode zstd, 
IG_CottageCode char(10) default '' encode zstd, 
IG_OwnOrLease char(10) default '' encode zstd, 
IG_Squarefootage8 char(10) default '' encode zstd, 
IG_WebAddress varchar(70) default '' encode zstd, 
IG_CallStatusCode char(10) default '' encode zstd, 
Fulfillment_Flag varchar(10) default '' encode zstd, 
IG_PhoneNumberType varchar(10) default '' encode zstd, 

--new fields for UCC combined
IG_CBSACode varchar (10) default '' encode zstd,
IG_CBSALevel varchar (10) default '' encode zstd,
IG_CSACode varchar (10) default '' encode zstd,
IG_LegalName varchar (120) default '' encode zstd,
IG_Mailing_SCF varchar(10) default '' encode zstd,  
IG_Mailing_StateCountyName varchar(60) default '' encode zstd,
IG_Mailing_StateCity varchar(150) default '' encode zstd,
IG_Mailing_StateCityZip varchar(150) default '' encode zstd,
IG_Mailing_StateCountyCode varchar(10) default '' encode zstd,
IG_Mailing_StateCitySCF varchar(150) default '' encode zstd,

--system and Calculated fields
FirstName  varchar(20) default '' encode zstd, 
LastName  varchar(20) default '' encode zstd, 
Gender char(10) default '' encode zstd,
AddressLine1  varchar(150) default '' encode zstd, 
City  varchar(28) default ' ' encode zstd, 
State  varchar(10) default ' ' encode zstd,  
Zip  varchar(10)  default ' ' encode zstd,
DT_Filing_YYYYMM varchar(10) default '' encode zstd,
DT_statecountycode varchar(5) default '' encode zstd,
DT_FullName varchar(150) default '' encode zstd, 
MATCHCODE_LRFS  varchar(39) default '' encode zstd,  
Has_IG_Mailing_Address char(1) default '' encode zstd,
Has_IGE_ContactName  char(1) default '' encode zstd,
Has_IG_WebAddress  char(1) default '' encode zstd,
Has_IG_PhoneNumber varchar(1) default '' encode zstd,
Has_DT_FullName char(1) default '' encode zstd, 
ListID int Default 8735 encode zstd,
BusinessEmailFlag  Char(1) Default  'N' encode zstd,
ListType char(1) default '' encode zstd,
PermissionType char(1) Default 'R' encode zstd,
ProductCode char(1) default '' encode zstd,
DT_AddressZip varchar(45) encode zstd,
TitleCode VARCHAR(5) default '' encode zstd, 
--dd tables columns
DT_CorporateIndividual_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_DeliverabilityScore_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_FilingState_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_FilingStatus_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_FilingTypeCode_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_USPSRecordType_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_MatchType_Description VARCHAR(150) DEFAULT '' encode zstd,
DT_CollateralType_Description VARCHAR(150) DEFAULT '' encode zstd,
IGE_TitleCode_Description VARCHAR(150) DEFAULT '' encode zstd,
IGE_VendorEthnicity_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_Mailing_State_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_MatchCode_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_EmployeeSizeCode2_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_OutputVolumeCode2_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_BusinessStatusCode_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_CreditAlphaScore_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_CottageCode_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_OwnOrLease_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_Squarefootage8_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_CallStatusCode_Description VARCHAR(150) DEFAULT '' encode zstd,
Fulfillment_Flag_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_CBSALevel_Description VARCHAR(150) DEFAULT '' encode zstd,
State_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_PrimarySIC6_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_SICCode1_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_SICCode2_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_SICCode3_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_SICCode4_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_NAICSCode_Description VARCHAR(150) DEFAULT '' encode zstd,
IG_CountyCode_Description VARCHAR(150) default '' encode zstd)

DISTKEY(ID)
SORTKEY(ID);