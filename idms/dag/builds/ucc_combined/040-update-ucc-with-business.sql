UPDATE {tablename3}
SET
IG_Company = B.Company,
IG_Location_Address=B.ADDRESS,
IG_Location_City=B.CITY,
IG_Location_State=B.STATE,
IG_Location_Zip=B.ZIP,
IG_Location_ZipFour=B.ZIPFOUR,
IG_Location_CarrierRouteCode=B.CARRIERROUTECODE,
IG_Mailing_Address=B.Mailing_ADDRESS,
IG_Mailing_City=B.Mailing_CITY,
IG_Mailing_State=B.Mailing_STATE,
IG_Mailing_StateCode= B.Mailing_StateCode,
IG_Mailing_Zip=B.Mailing_ZIPCode,
IG_Mailing_ZipFour=B.Mailing_ZIPFOUR,
IG_Mailing_CarrierRouteCode=B.MAILING_CARRIEROUTECODE,
IG_CountyCode=B.COUNTYCODE,
IG_CensusTract=B.CENSUSTRACT,
IG_CensusBlock=B.CENSUSBLOCK,
fLATITUDE=B.fLATITUDE, --CAST(B.fLATITUDE AS VARCHAR(10)),
fLONGITUDE=B.fLONGITUDE, --CAST(B.fLONGITUDE AS VARCHAR(11)),
IG_MatchCode=B.MATCHCODE,
IG_PhoneNumber=B.PHONENUMBER,
IG_FaxPhone=B.FAXPHONE,
IG_PrimarySIC6=B.PRIMARYSIC6,
IG_SICCode1=B.SICCODE1,
IG_SICCode2=B.SICCODE2,
IG_SICCode3=B.SICCODE3,
IG_SICCode4=B.SICCODE4,
IG_NAICSCode=B.NAICSCODE , 
IG_NumberEmployees=B.NUMBEREMPLOYEES,--cast(B.NUMBEREMPLOYEES as varchar),
IG_EmployeeSizeCode2=B.EMPLOYEESIZECODE2,
IG_OutputVolume2=B.OUTPUTVOLUME2, --Inferred_Corporate_Sales_Volume_Amount,--B.CORPORATEOUTPUTVOLUME,
IG_OutputVolumeCode2=B.OUTPUTVOLUMECODE2,
IG_CreditNumericScore=B.CREDITNUMERICSCORE,
IG_CreditAlphaScore=B.CREDITALPHASCORE,
IG_BusinessStatusCode=B.BUSINESSSTATUSCODE  ,
IG_CallStatusCode=B.CALLSTATUSCODE, 
IG_CottageCode=B.COTTAGECODE,
Ig_OwnOrLease=B.OWNORLEASE,
IG_ParentABINumber=B.PARENTABINUMBER,
IG_SubsidairyABINumber=B.SUBSIDAIRYABINUMBER ,
IG_WebAddress=B.WEBADDRESS ,
IG_YearFirstAppeared=B.YEARFIRSTAPPEARED,
IG_CBSACode=B.CBSACODE,
IG_CBSALevel=B.CBSALEVEL,
IG_CSACode=B.CSACODE,
FULFILLMENT_FLAG=B.FULFILLMENTFLAG,
IG_SquareFootage8=B.SQUAREFOOTAGE8,
IG_PhoneNumberType=B.PHONE_NUMBER_TYPE,
IG_LegalName=B.LEGAL_NAME,
IG_Mailing_SCF = B.SCF, --add here
AddressLine1 = B.Mailing_Address,
City = B.Mailing_City,
State = B.Mailing_State,
Zip = B.Mailing_Zipcode,
IG_Mailing_StateCountyName = B.StateCountyName,
IG_Mailing_StateCity = B.CityByState,
IG_Mailing_StateCityZip = B.StateCityZip,
IG_Mailing_StateCountyCode = B.StateCountyCode, 
IG_Mailing_StateCitySCF = B.StateCitySCF

FROM {tablename3} A INNER JOIN tblBusinessIndividual_ctas1 B 
ON A.DT_ABINumber=B.ABINumber where B.ID IN (SELECT MIN(ID) AS ID FROM tblBusinessIndividual_ctas1 GROUP BY ABINumber);

UPDATE {tablename3}
SET 
IGE_FullName=B.fullname,
	IGE_TitleCode=B.titlecode,
	IGE_TitleCode_Back=B.titlecode_Back,
	IGE_VendorEthnicity=B.VENDOR_ETHNICITY,
	IGE_ContactID=B.contactid,
	FirstName =B.FirstName,
	LastName = B.LastName,
	Gender = B.Gender
FROM {tablename3} A INNER JOIN tblBusinessIndividual_ctas1 B --dba.tblMain_18403_201911 B
ON A.DT_ABINumber=B.ABINumber WHERE
B.ID IN (SELECT MIN(ID) AS ID FROM tblBusinessIndividual_ctas1 where typecode='C'   GROUP BY ABINumber);

UPDATE {tablename3}   SET TITLECODE =	CASE
				WHEN IGE_TitleCode = '!'  THEN 'Z1'
				WHEN IGE_TitleCode = '#'  THEN 'Z2'
				WHEN IGE_TitleCode = '$'  THEN 'Z3'
				WHEN IGE_TitleCode = '%'  THEN 'Z4'
				WHEN IGE_TitleCode = '&'  THEN 'Z5'
				WHEN IGE_TitleCode = '('  THEN 'Z6'
				WHEN IGE_TitleCode = ')'  THEN 'Z7'
				WHEN IGE_TitleCode = '+'  THEN 'Z8'
				WHEN IGE_TitleCode = '-'  THEN 'Z9'
				WHEN IGE_TitleCode = '.'  THEN 'Z10'
				WHEN IGE_TitleCode = '/'  THEN 'Z11'
				WHEN IGE_TitleCode = ':'  THEN 'Z12'
				WHEN IGE_TitleCode = '='  THEN 'Z13'
				WHEN IGE_TitleCode = '>'  THEN 'Z14'
				WHEN IGE_TitleCode = '?'  THEN 'Z15'
				WHEN IGE_TitleCode = '@'  THEN 'Z16'
				WHEN IGE_TitleCode = '['  THEN 'Z17'
				WHEN IGE_TitleCode = '\\'  THEN 'Z18'
				WHEN IGE_TitleCode = ']'  THEN 'Z19'
				WHEN IGE_TitleCode = '^'  THEN 'Z20'
				WHEN IGE_TitleCode = '_'  THEN 'Z21'
				WHEN IGE_TitleCode = '{'  THEN 'Z22'
				ELSE IGE_TitleCode END; 
