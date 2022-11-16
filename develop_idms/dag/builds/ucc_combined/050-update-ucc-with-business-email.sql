
DROP TABLE IF EXISTS TempBusiness_ToBeDropped; 

--Pulll all from Business Email
Select DISTINCT ABI_Number_LRFS, LEFT(Company_Name_LRFS,40) as Company_Name_LRFS, LEFT(FullName,30) as FullName
into TempBusiness_ToBeDropped  
FROM  business_email_table_ucc
--FROM dba.tblMain_18719_202003
where LTRIM(RTRIM(Email_Address_LRFS)) <>'' ;  
commit;


--Match Criteria 1   Company  & ABI Num
--DEBTRNM_LRFS is no longer loaded 
UPDATE  {tablename3}
  SET BusinessEmailFlag = 'Y'
--Select count(*)
From  {tablename3} u
inner join TempBusiness_ToBeDropped Business  ON u.DT_ABINumber =Business.ABI_Number_LRFS  AND u.DT_Company =Business.Company_Name_LRFS
where (u.BusinessEmailFlag is NULL or u.BusinessEmailFlag <>'Y');
Commit;


--Match Criteria 2   Full Name  & ABI Num
--CONTACTMRG_LRFS is no longer in the input file and is loaded at UPDATE UCC COMBINED WITH 992 EXEC task
UPDATE  {tablename3}
  SET BusinessEmailFlag = 'Y'
--Select count(*)
From  {tablename3} u
inner join TempBusiness_ToBeDropped Business  ON u.DT_ABINumber =Business.ABI_Number_LRFS  AND u.IGE_FullName =Business.FullName
where (u.BusinessEmailFlag is NULL or u.BusinessEmailFlag <>'Y');
Commit;


UPDATE {tablename3}   
SET 
DT_Filing_YYYYMM = DT_Filing_Year+DT_Filing_Month, 
MATCHCODE_LRFS = DT_ABINumber+DT_SourceFilingID, 
Has_IG_Mailing_Address = CASE WHEN RTRIM(IG_Mailing_Address) = '' THEN 'N' ELSE 'Y' END,--ADDRESS_AVAILABLE_LRFS = CASE WHEN RTRIM(IG_Mailing_Address) = '' THEN 'N' ELSE 'Y' END, 
Has_IGE_ContactName=CASE WHEN RTRIM(IGE_FullName) = '' THEN 'N' ELSE 'Y' END, --CONTACT_NAME_AVAILABLE_LRFS = CASE WHEN RTRIM(IGE_ContactManager) = '' THEN 'N' ELSE 'Y' END, 
Has_IG_WebAddress=CASE WHEN RTRIM(IG_WebAddress) = '' THEN 'N' ELSE 'Y' END, --WEB_ADDRESS_AVAILABLE_LRFS = CASE WHEN RTRIM(IG_WebAddress) = '' THEN 'N' ELSE 'Y' END, 
Has_IG_PhoneNumber = CASE WHEN (ISNULL(IG_PhoneNumber,'') ='' AND LTRIM(RTRIM(IG_FaxPhone)) = '') THEN 'N' ELSE 'Y' END,
IG_OutputVolumeCode2 = CASE WHEN  (IG_OutputVolumeCode2 IS NULL OR RTRIM(IG_OutputVolumeCode2) = '') THEN 'Z' ELSE IG_OutputVolumeCode2 END,
DT_statecountycode= trim(DT_State) + trim(DT_CountyCode),
Has_DT_FullName = CASE WHEN LTRIM(RTRIM(DT_FirstName+DT_LastName))='' THEN 'N' ELSE 'Y' END,
DT_FullName=trim(DT_FirstName) + ' ' + trim(DT_LastName),
DT_CollateralType_Comb = 	trim(DT_CollateralType) +  trim(DT_CollateralType2) +  trim(DT_CollateralType3) 
						+ trim(DT_CollateralType4) +  trim(DT_CollateralType5) +  trim(DT_CollateralType6) 
						+ trim(DT_CollateralType7) +  trim(DT_CollateralType8) +  trim(DT_CollateralType9) 
						+ trim(DT_CollateralType10),

DT_CollateralType_Multi = ',' + DT_CollateralType + ',' + DT_CollateralType2 + ',' + DT_CollateralType3 +
					',' + DT_CollateralType4 + ',' + DT_CollateralType5 + ',' + DT_CollateralType6 +
					',' + DT_CollateralType7 + ',' + DT_CollateralType8 + ',' + DT_CollateralType9 +
					',' + DT_CollateralType10 + ','
;