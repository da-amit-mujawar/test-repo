INSERT INTO tblBusinessIndividual
(
    ABINUMBER,
    ABI_AVAILABLE, 
    ADDRESS,
    CITY,
    STATE,
    ZIP_Back,
    ZIPFOUR,
    ZIP9,
    CARRIERROUTECODE,
    MAILCONFIDENCE,
    ADSIZECODE,
    BADEMAILS,
    BUSINESSSTATUSCODE,
    CALLSTATUSCODE,
    CBSACODE,
    CDOMAIN,
    CDOMAINTYPE,
    CENSUSTRACT,
    CINCLUDE,
    CITY_AVAILABLE,
    COMPANY,
    COMPANY_AVAILABLE,
    CONTACTS_PER_COMPANY,
    COUNTYCODE,
    CREDITALPHASCORE,
    CREDITNUMERICSCORE,
    CSACODE,
    EMAILADDRESS,
    EMPLOYEESIZECODE2,
    EXECUTIVESOURCECODE,
    FAXPHONE,
    FIRSTNAME,
    FULLNAME,
    GENDER,
    GOVERNMENTFLAG,
    GROWINGBUSINESSFLAG,
    Haspostal,
    INFOGROUP_EMAILS,
    LASTNAME,
    LISTID,
    MD5LOWER,
    MD5UPPER,
    MD5SaltedHash,
    SHA256Lower,
    SHA256Upper,
    MULTITENANTCODE,
    BUILDINGNUMBER,
    NAME_AVAILABLE,
    OFFICESIZECODE,
    ONE_PER_ABI,
    ONE_PER_ABINAME,
    ONE_PER_COMPANY_NAME_TITLE,
    ONE_PER_CONTACT_NAME,
    OUTPUTVOLUMECODE2,
    OWNORLEASE,
    PERMISSIONTYPE,
    PHONENUMBER,
    PHONE_NUMBER_AVAILABLE,
    PROFTITLE,
    ROYALTY_EMAILS,
    SICCODE1,
    SICCODE2,
    SICCODE3,
    SICCODE4,
    SQUAREFOOTAGE8,
    TELERESEARCHUPDATEDATE,
    TITLECODE,
    VENDOR_ID,
    WEBADDRESS,
    WEBSITE_AVAILABLE,
    YEARFIRSTAPPEARED,
    BVT_EMAIL_STATUS_Email,
    DEPARTMENT_CODE_Email,
    DEPARTMENT_CODE_MULTI_Email,
    FRANCHISE_CODE_Email,
    INDIVIDUAL_FIRM_Email,
    JOB_LEVEL_CODE_Email,
    JOB_LEVEL_CODE_MULTI_Email,
    MAIL_SCORE_Email,
    --ONEPERFLAG_Email, not in tblmain
    ONE_PER_ABI_EMAIL_Email,
    ONE_PER_COMPANY_Email,
    ONE_PER__ABI_Email,
    PRODUCTCODE_Email,
    COMPANYHOLDINGSTATUS,
    RESPONDER_DATE_Email,
    SEQ_Email,
    SUPPRESSION_TYPE_Email,
    TITLE_DESCRIPTION_Email,
    TITLE_RANK_Email,
    TRANSACTION_DATE_Email,
    TRANSACTION_TYPE_Email,
    ULTIMATE_NUMBER_Email,
    YEAR_SIC_ADDED_Email,
    fLATITUDE,
    fLONGITUDE,
    Geo_Match_Level,
    Database_Flag,
    Work_At_Home_Indicator,
    SITENUMBER,
    Phone_Number_Type,
    FULFILLMENTFLAG,
    ContactID,
    PARENTABINUMBER,
    DigitalMatch,
    Functional_Area_Code,
    Department_Code,
    Role_Code,
    Level_Code,
    ContactQuality, 
    LiteralTitle_ID,
    CENSUSBLOCK,
    Inferred_Corporate_Employee_Count,
    Inferred_Corporate_Employee_Range,
    Inferred_Corporate_Employee_Source_Indicator,
    Inferred_Corporate_Sales_Volume_Amount,
    Inferred_Corporate_Sales_Volume_Range,
    Inferred_Corporate_Sales_Volume_Source_Indicator,
    NAICSCODE,
    NUMBEREMPLOYEES,
    OUTPUTVOLUME2,
    SUBSIDAIRYABINUMBER,
    YEARESTABLISHED,
    CREDITCARDSACCEPTED,
    ADDRESSTYPE,
    FEMALEEXECFLAG,
    TRUEFRANCHISEFLAG,
    INDUSTRYSPECIFICFIRSTBYTE,
    WEALTHCODE,
    BIGBUSINESSSEGMENTATIONCODE,
    SMMEDBUSSEG1,
    SMMEDBUSSEG2,
    FORTUNERANKING,
    HIGHTECHBUSINESSFLAG,
    FILINGFLAG,
    WHITECOLLAR,
    WHITECOLLARFLAG,
    FOREIGNPARENTFLAG,
    IMPORTEXPORTFLAG,
    HIGHINCOMEEXECFLAG,
    GREENADOPTERSCORE,
    TELECOMMUNICATIONS_EXPENSE,
    TECHNOLOGY_EXPENSE,
    OFFICE_EQUIPMENT_EXPENSE,
    PAYROLL_EXPENSE,
    RENT_LEASING_EXPENSE,
    ADVERTISING_EXPENSE,
    UTILITIES_EXPENSE,
    ACCOUNTING_EXPENSE,
    PACKAGECONTAINER_EXPENSE,
    CONTRACT_LABOR_EXPENSE,
    INSURANCE_EXPENSE,
    PURCHASED_PRINT_EXPENSE,
    PURCHASE_MGMTADMIN_SVC_EXPENSE,
    LEGAL_EXPENSE,
    CHARITABLE_DONTATIONS_EXPENSE,
    LICENSE_FEES_TAXES,
    MAINTENANCE_REPAIR,
    TransportationExpenseCode,
    FRANCHISE_CODE_1,
    FRANCHISE_CODE_2,
    FRANCHISE_CODE_3,
    FRANCHISE_CODE_4,
    FRANCHISE_CODE_5,
    FRANCHISE_CODE_6,
    FLEETSIZE,
    LEGAL_NAME,
    LAST_DATE,
    VENDOR_ETHNICITY,
    VENDOR_ETHNIC_GROUP,
    VENDOR_LANGUAGE,
    VENDOR_RELIGION,
    Creditcardsaccepted_Multi,
    Bridge_Code,
    Domain_ID,
    TopLevelDomain,
    Email_Marketable,
    Email_Deliverable,
    Email_Reputation_Risk, 
    MAILING_ADDRESS,
    MAILING_CITY,
    MAILING_STATE, 
    MAILING_ZIPCODE,
    MAILING_ZIPFOUR, 
    MAILING_CARRIEROUTECODE,
    MAILCONFIDENCE_MAILING
)
SELECT
    LEFT(B.ABI_Number_LRFS,9),
   LEFT(B.ABI_Available_LRFS,1),
    CASE WHEN C.Address IS NULL THEN LEFT(B.AddressLine1,30) ELSE C.Address END,
    CASE WHEN C.CITY IS NULL THEN LEFT(B.CITY,16) ELSE C.CITY END,
    CASE WHEN C.STATE IS NULL THEN LEFT(B.STATE,2) ELSE C.STATE END,
    CASE WHEN C.ZIP IS NULL THEN LEFT(B.ZIP,5) ELSE C.ZIP END,
    CASE WHEN C.ZIPFOUR IS NULL THEN LEFT(B.ZIP4_LRFS,4) ELSE C.ZIPFOUR END,
    CASE WHEN C.ZIP9 IS NULL THEN LEFT(NVL(B.ZIP,'')+NVL(B.Zip4_LRFS,''),9)   ELSE C.ZIP9 END,
    CASE WHEN C.CARRIERROUTECODE IS NULL THEN LEFT(B.CARRIER_ROUTE_LRFS,4) ELSE C.CARRIERROUTECODE   END,
    CASE WHEN C.MAILCONFIDENCE IS NULL THEN cast(LEFT(B.Primary_Mail_Confidence,1) as varchar(1)) ELSE cast(C.MAILCONFIDENCE as varchar(1))  END,
    cast(LEFT(B.Ad_Size_LRFS,1) as varchar(1)),
    LEFT(B.BADEMAILS,1),
    cast(LEFT(B.BUSINESS_STATUS_CODE_LRFS,1) as varchar(1)),
    cast(LEFT(B.CALL_STATUS_LRFS,1) as varchar(1)),
    LEFT(B.CBSA_LRFS,5),
    LEFT(B.DOMAIN_NAME_LRFS,80),
    LEFT(B.CDOMAIN,1),
    LEFT(B.CENSUS_LRFS,6),
    LEFT(B.CINCLUDE,1),
    cast(LEFT(B.CITY_AVAILABLE,1) as varchar(1)),
    LEFT(B.COMPANY_NAME_LRFS,30),
    cast(LEFT(B.COMPANY_AVAILABLE_LRFS,1) as varchar(1)),
    LEFT(B.CONTACTS_PER_COMPANY_LRFS,60),
    LEFT(B.COUNTY_CODE_LRFS,3),
    LEFT(B.BUSINESS_CREDIT_SCORE_CODE_LRFS,2),
    LEFT(B.BUSINESS_CREDIT_SCORE_LRFS,3),
    LEFT(B.CSA_LRFS,3),
    LEFT(B.EMAIL_ADDRESS_LRFS,60),
    cast(LEFT(B.EMPLOYEE_SIZE_CODE_LRFS,1) as varchar(1)),
    LEFT(B.EXECUTIVE_SOURCE_CODE_LRFS,2),
    LEFT(B.FAX_NUMBER_LRFS,10),
    LEFT(B.FIRSTNAME,11),
    LEFT(B.FULLNAME,40),
    cast(LEFT(B.GENDER_LRFS,1) as varchar(1)),
    cast(LEFT(B.GOVERNMENT_SEGMENT_CODE_LRFS,1) as varchar(1)),
    cast(LEFT(B.GROWING_SHRINKING_INDICATOR_LRFS,1) as varchar(1)),
    'N',
    LEFT(B.INFOGROUP_EMAILS_LRFS,1),
    LEFT(B.LASTNAME,14),
    cast(B.LISTID AS INTEGER),
    LEFT(B.MD5LOWER_LRFS,32),
    LEFT(B.MD5UPPER_LRFS,32),   
    LEFT(B.MD5SaltedHash,32 ),
    LEFT(B.SHA256Lower,64),
    LEFT(B.SHA256Upper,64), 
    cast(LEFT(B.MULTI_TENANT_CODE_LRFS,1) as varchar(1)),
    LEFT(B.MULTI_TENANT_BUILDING_NUMBER_LRFS,7),
    cast(LEFT(B.NAME_AVAILABLE_LRFS,1) as varchar(1)),
    cast(LEFT(B.OFFICE_SIZE_LRFS,1) as varchar(1)),
    LEFT(B.ONE_PER_ABI_LRFS,14),
    LEFT(B.ONE_PER_ABINAME_LRFS,40),
    LEFT(B.ONE_PER_COMPANY_NAME_TITLE,35),
    LEFT(B.ONE_PER_CONTACT_NAME_LRFS,40),
    cast(LEFT(B.SALES_VOLUME_CODE_LRFS,1) as varchar(1)),
    cast(LEFT(B.OWN_LEASE_LRFS,1) as varchar(1)),
    LEFT(B.PERMISSIONTYPE,1),
    LEFT(B.A10_DIGIT_PHONE_LRFS,10),
    cast(LEFT(B.PHONE_NUMBER_AVAILABLE_LRFS,1) as varchar(1)),
    LEFT(B.PROFESSIONAL_TITLE_CODE,3),
    LEFT(B.ROYALTY_EMAILS_LRFS,1),
    LEFT(B.SECONDARY_SIC_CODE_LRFS,6),
    LEFT(B.SECONDARY_SIC2_LRFS,6),
    LEFT(B.SECONDARY_SIC3_LRFS,6),
    LEFT(B.SECONDARY_SIC4_LRFS,6),
    cast(LEFT(B.SQUARE_FOOTAGE_LRFS,1) as varchar(1)),
    LEFT(B.TELERESEARCH_DATE_LRFS,6),
    LEFT(B.TITLE_LRFS,3),
    LEFT(B.VENDOR_ID_LRFS,2),
    LEFT(B.WEBSITE_LRFS,40),
    cast(LEFT(B.WEBSITE_AVAILABLE,1) as varchar(1)),
    LEFT(B.YEAR_FIRST_APPEARED_LRFS,4),
    LEFT(B.BVT_EMAIL_STATUS_LRFS,1),
    LEFT(B.DEPARTMENT_CODE,4),
    LEFT(B.DEPARTMENT_CODE_MULTI,9),
    LEFT(B.FRANCHISE_CODE_LRFS,6),
    LEFT(B.INDIVIDUAL_FIRM_LRFS,1),
    LEFT(B.JOB_LEVEL_CODE,4),
    LEFT(B.JOB_LEVEL_CODE_MULTI,9),
    LEFT(B.MAIL_SCORE,2),
    --LEFT(B.ONEPERFLAG,1),
    LEFT(B.ONE_PER_ABI_EMAIL_LRFS,69),
    LEFT(B.ONE_PER_COMPANY_LRFS,30),
    LEFT(B.ONE_PER__ABI_LRFS,1),
    LEFT(B.PRODUCTCODE,1),
    cast(LEFT(B.PUBLIC_PRIVATE_CODE_LRFS,1) as varchar(1)),
    LEFT(B.RESPONDER_DATE,6),
    LEFT(B.SEQ_LRFS,9),
    LEFT(B.SUPPRESSION_TYPE,1),
    LEFT(B.TITLE_DESCRIPTION_LRFS,150),
    LEFT(B.TITLE_RANK_LRFS,2),
    LEFT(B.TRANSACTION_DATE_LRFS,6),
    LEFT(B.TRANSACTION_TYPE_LRFS,1),
    LEFT(B.ULTIMATE_NUMBER_LRFS,9),
    LEFT(B.YEAR_SIC_ADDED_LRFS,6), 
    B.fLATITUDE,
    B.fLONGITUDE , 
    LEFT(B.Geo_Match_Level,1), 
    LEFT(B.Database_Flag,1), 
    LEFT(B.Work_At_Home_Indicator,1), 
    LEFT(B.SITENUMBER,9),
    cast(b.Phone_Number_Type as varchar(1)),
    cast(b.Database_Flag as varchar(1)),
    b.ContactID,
    b.Ultimate_Number_LRFS,
    cast(b.DigitalMatch as varchar(1)),
    b.Functional_Area_Code,
    b.Literal_Title_Department_Code,
    b.Role_Code,
    cast(b.Level_Code as varchar(1)),
    cast(b.ContactQuality as varchar(1)),
    B.LiteralTitle_ID, 
    cast(B.CENSUSBLOCK as varchar(1)),
    CASE WHEN (LTRIM(RTRIM(B.Inferred_Corporate_Employee_Count))='' or B.Inferred_Corporate_Employee_Count is NULL) THEN 0 ELSE CAST (B.Inferred_Corporate_Employee_Count as int) END,
    B.Inferred_Corporate_Employee_Range,
    B.Inferred_Corporate_Employee_Source_Indicator,
    CASE WHEN (LTRIM(RTRIM(B.Inferred_Corporate_Sales_Volume_Amount))=''  OR B.Inferred_Corporate_Sales_Volume_Amount IS NULL) THEN 0 ELSE CAST (B.Inferred_Corporate_Sales_Volume_Amount as int) END,
    B.Inferred_Corporate_Sales_Volume_Range,
    B.Inferred_Corporate_Sales_Volume_Source_Indicator,
    B.NAICSCODE,
    CASE WHEN (LTRIM(RTRIM(B.NUMBEREMPLOYEES))=''  OR B.NUMBEREMPLOYEES IS NULL) THEN 0 ELSE CAST (B.NUMBEREMPLOYEES as int) END,
    CASE WHEN (LTRIM(RTRIM(B.OUTPUTVOLUME2))=''  OR B.OUTPUTVOLUME2 IS NULL) THEN 0 ELSE CAST (B.OUTPUTVOLUME2 as int) END,
    B.SUBSIDAIRYABINUMBER,
    B.YEARESTABLISHED,
    B.CREDITCARDSACCEPTED,
    B.ADDRESSTYPE,
    cast(B.FEMALEEXECFLAG as varchar(1)),
    cast(B.TRUEFRANCHISEFLAG as varchar(1)),
    cast(B.INDUSTRYSPECIFICFIRSTBYTE as varchar(1)),
    Cast(B.WEALTHCODE as varchar(1)),
    Cast(B.BIGBUSINESSSEGMENTATIONCODE as varchar(1)),
    cast(B.SMMEDBUSSEG1 as varchar(1)),
    cast(B.SMMEDBUSSEG2 as varchar(1)),
    B.FORTUNERANKING,
    cast(B.HIGHTECHBUSINESSFLAG as varchar(1)),
    cast(B.FILINGFLAG as varchar(1)),
    B.WHITECOLLAR,
    cast(B.WHITECOLLARFLAG as varchar(1)),
    cast(B.FOREIGNPARENTFLAG as varchar(1)),
    cast(B.IMPORTEXPORTFLAG as varchar(1)),
    cast(B.HIGHINCOMEEXECFLAG as varchar(1)),
    cast(B.GREENADOPTERSCORE as varchar(1)),
    cast(B.TELECOMMUNICATIONS_EXPENSE as varchar(1)),
    cast(B.TECHNOLOGY_EXPENSE as varchar(1)),
    cast(B.OFFICE_EQUIPMENT_EXPENSE as varchar(1)),
    cast(B.PAYROLL_EXPENSE as varchar(1)),
    cast(B.RENT_LEASING_EXPENSE as varchar(1)),
    cast(B.ADVERTISING_EXPENSE as varchar(1)),
    cast(B.UTILITIES_EXPENSE as varchar(1)),
    cast(B.ACCOUNTING_EXPENSE as varchar(1)),
    cast(B.PACKAGECONTAINER_EXPENSE as varchar(1)),
    cast(B.CONTRACT_LABOR_EXPENSE as varchar(1)),
    cast(B.INSURANCE_EXPENSE as varchar(1)),
    cast(B.PURCHASED_PRINT_EXPENSE as varchar(1)),
    cast(B.PURCHASE_MGMTADMIN_SVC_EXPENSE as varchar(1)),
    cast(B.LEGAL_EXPENSE as varchar(1)),
    cast(B.CHARITABLE_DONTATIONS_EXPENSE as varchar(1)),
    cast(B.LICENSE_FEES_TAXES as varchar(1)),
    cast(B.MAINTENANCE_REPAIR as varchar(1)),
    cast(B.TransportationExpenseCode as varchar(1)),
    B.FRANCHISE_CODE_1,
    B.FRANCHISE_CODE_2,
    B.FRANCHISE_CODE_3,
    B.FRANCHISE_CODE_4,
    B.FRANCHISE_CODE_5,
    B.FRANCHISE_CODE_6,
    cast(B.FLEETSIZE as varchar(1)),
    B.Legal_Name,
    B.Last_Modified_Date,
    B.VENDOR_ETHNICITY,
    cast(B.VENDOR_ETHNIC_GROUP as varchar(1)),
    B.VENDOR_LANGUAGE,
    cast(B.VENDOR_RELIGION as varchar(1)),
    B.Creditcardsaccepted_Multi,
    B.Bridge_Code,
    B.Domain_ID,
    B.TopLevelDomain,
    cast(B.Email_Marketable as varchar(1)),
    cast(B.Email_Deliverable as varchar(1)), 
    cast(B.Email_Reputation_Risk as varchar(1)),
    LEFT(B.AddressLine1,30),
    LEFT(B.CITY,16),
    LEFT(B.STATE,2),
    LEFT(B.ZIP,5),
    LEFT(B.ZIP4_LRFS,4),
    LEFT(B.CARRIER_ROUTE_LRFS,4),
    cast(LEFT(B.Primary_Mail_Confidence,1) as varchar(1))

FROM business_email_table B
LEFT JOIN 
(
  SELECT ADDRESS,CITY,STATE,ZIP,ZIPFOUR,ZIP9,CARRIERROUTECODE,ABINUMBER,MAILCONFIDENCE FROM tblBusinessCompany
  ) C
ON B.ABI_Number_LRFS = C.ABINUMBER 
WHERE ContactID NOT IN 
(
  SELECT ContactID FROM tblBusinessIndividual 
   WHERE ContactID IS NOT NULL
   GROUP BY ContactID
)
and (B.ContactID is not null and B.ContactID <>'');