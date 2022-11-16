UPDATE tblBusinessIndividual
   SET EmailAddress = B.Email_Address_LRFS,
         cDomain = B.Domain_name_LRFS,
         cDomainType = CASE WHEN B.cDomain = 'YAHOO.COM' THEN 'Y'  
	                        WHEN B.cDomain = 'HOTMAIL.COM' THEN 'H' 
 	                        WHEN B.cDomain = 'GMAIL.COM' THEN  'G'
         	               WHEN B.cDomain = 'AOL.COM' THEN  'A'
         	               WHEN B.cDomain = 'OTHER' THEN  'O' END,
         BadEmails = B.BadEmails,
         Royalty_Emails = B.Royalty_Emails_LRFS,
         Infogroup_Emails = B.Infogroup_Emails_LRFS,
         Vendor_ID = B.Vendor_ID_LRFS,
         MD5Upper = b.MD5Upper_LRFS,
         MD5Lower = b.MD5Lower_LRFS,
         MD5SaltedHash = B.MD5SaltedHash,
         SHA256Lower = B.SHA256Lower,
         SHA256Upper = B.SHA256Upper,
         cInclude = B.cInclude,
         BVT_EMAIL_STATUS_Email = CASE WHEN (B.BVT_EMAIL_STATUS_LRFS IS NOT NULL) THEN  LEFT(B.BVT_EMAIL_STATUS_LRFS,1) 
                                   ELSE B.BVT_EMAIL_STATUS_LRFS END,
         Bridge_Code = B.Bridge_Code,
         Domain_ID = B.Domain_ID,	
         TopLevelDomain = B.TopLevelDomain,
         Email_Marketable = B.Email_Marketable,
         Email_Deliverable = B.Email_Deliverable, 
         Email_Reputation_Risk = B.Email_Reputation_Risk, 
         Geo_Match_Level = CASE WHEN (A.Geo_Match_Level IS NULL OR LTRIM(RTRIM(A.Geo_Match_Level)) ='' ) THEN B.Geo_Match_Level ELSE A.Geo_Match_Level END,
         Database_Flag = CASE WHEN (A.Database_Flag IS NULL OR LTRIM(RTRIM(A.Database_Flag)) ='' ) THEN  B.Database_Flag ELSE A.Database_Flag END,
         Work_At_Home_Indicator = CASE WHEN (A.Work_At_Home_Indicator IS NULL OR LTRIM(RTRIM(A.Work_At_Home_Indicator)) ='') THEN B.Work_At_Home_Indicator ELSE A.Work_At_Home_Indicator END,
         EXECUTIVESOURCECODE = CASE WHEN (A.EXECUTIVESOURCECODE IS NULL OR LTRIM(RTRIM(A.EXECUTIVESOURCECODE)) =''   ) THEN B.Executive_Source_Code_LRFS ELSE A.EXECUTIVESOURCECODE END,  
         PARENTABINUMBER = CASE WHEN (A.PARENTABINUMBER IS NULL OR LTRIM(RTRIM(A.PARENTABINUMBER)) ='' ) THEN B.Ultimate_Number_LRFS ELSE A.PARENTABINUMBER END,  
         LiteralTitle_ID = CASE WHEN (A.LiteralTitle_ID IS NULL OR LTRIM(RTRIM(A.LiteralTitle_ID)) ='' ) THEN B.LiteralTitle_ID ELSE A.LiteralTitle_ID END
FROM tblBusinessIndividual  A
Inner join 
business_email_table B
on A.ContactID = B.ContactID AND A.ABINUMBER = B.ABI_Number_LRFS
where (A.EmailAddress  ='' or a.EmailAddress is null) AND 
(B.Email_Address_Lrfs <>'' and b.Email_Address_Lrfs is not null)
and B.cInclude ='Y' 
and (B.ContactID is not null and B.ContactID <>'')
;



-----------------------------------
UPDATE tblBusinessIndividual
   SET EmailAddress = B.Email_Address_LRFS,
      cDomain = B.Domain_name_LRFS,
      cDomainType = CASE WHEN B.cDomain = 'YAHOO.COM' THEN 'Y'
                        WHEN B.cDomain = 'HOTMAIL.COM' THEN 'H' 
                        WHEN B.cDomain = 'GMAIL.COM' THEN  'G'
                        WHEN B.cDomain = 'AOL.COM' THEN  'A'
                        WHEN B.cDomain = 'OTHER' THEN  'O' END,
      BadEmails =B.BadEmails,
      Royalty_Emails= B.Royalty_Emails_LRFS,
      Infogroup_Emails =B.Infogroup_Emails_LRFS,
      Vendor_ID =B.Vendor_ID_LRFS,
      MD5Upper =b.MD5Upper_LRFS,
      MD5Lower =b.MD5Lower_LRFS,
      SHA256Lower =B.SHA256Lower,
      SHA256Upper =B.SHA256Upper,
      MD5SaltedHash = B.MD5SaltedHash,
      cInclude = B.cInclude,
      BVT_EMAIL_STATUS_Email = CASE WHEN (B.BVT_EMAIL_STATUS_LRFS IS NOT NULL) THEN  LEFT(B.BVT_EMAIL_STATUS_LRFS,1) ELSE B.BVT_EMAIL_STATUS_LRFS END,
      Bridge_Code =B.Bridge_Code,
      Domain_ID =B.Domain_ID,	
      TopLevelDomain =B.TopLevelDomain,
      Email_Marketable=B.Email_Marketable,
      Email_Deliverable=B.Email_Deliverable, 
      Email_Reputation_Risk=B.Email_Reputation_Risk
FROM tblBusinessIndividual  A
Inner join 
business_email_table  b
on A.ContactID = B.ContactID
where (A.EmailAddress  ='' or a.EmailAddress is null)
AND (B.Email_Address_Lrfs <>'' AND B.Email_Address_Lrfs is not null) and B.cInclude ='Y' 
and (B.ContactID is not null and B.ContactID <>'')
;