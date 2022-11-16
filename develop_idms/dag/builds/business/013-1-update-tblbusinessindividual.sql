UPDATE tblBusinessIndividual --28min 297055422
   SET fLATITUDE  = cast(LEFT(RIGHT('000' + NVL(CAST(cast(fLATITUDE_old as int)  as varchar),''),9),3) + '.' + RIGHT(RIGHT('000' + NVL(CAST(cast(fLATITUDE_old  as int) as varchar),''),9),6) as float),
     	fLONGITUDE = cast(LEFT(RIGHT('000' + NVL(CAST(cast(fLONGITUDE_old as int) as varchar),''),9),3) + '.' + RIGHT(RIGHT('000' + NVL(CAST(cast(fLONGITUDE_old as int) as varchar),''),9),6) as float),
		FullName = LTRIM(RTRIM(nvl(FirstName,'')))+ ' ' + LTRIM(RTRIM(nvl(LastName,''))), 
		One_per_ABI = LEFT(NVL(Zip_back,''),5)+RTRIM(nvl(ABINumber,'')), 
		One_per_ABIName = LEFT(NVL(ABINumber,''),9)+ltrim(RTRIM(NVL(FirstName,'')))+ltrim(rtrim(NVL(LastName,''))), 
		One_per_Contact_Name = LEFT(nvl(Zip_back,''),5)+ltrim(RTRIM(nvl(FirstName,'')))+ltrim(rtrim(nvl(LastName,''))), 
		Contacts_Per_Company = LEFT(LTRIM(RTRIM(NVL(Company,''))),30)+RTRIM(LTRIM(RTRIM(NVL(FirstName,'')))+LTRIM(RTRIM(NVL(LastName,'')))), 
		Name_Available = CASE WHEN LTRIM(RTRIM(nvl(FirstName,'')))+ LTRIM(RTRIM(nvl(LastName,''))) = '' OR (FirstName is NULL and LastName is NUll) THEN 'N' ELSE 'Y' END, 
		Company_Available = CASE WHEN LTRIM(RTRIM(Company)) = '' THEN 'N' ELSE 'Y' END, 
		ABI_Available = CASE WHEN (ABINumber = '000000000' OR LTRIM(RTRIM(ABINUMBER)) ='' ) THEN 'N' ELSE 'Y' END, 
		Phone_Number_Available = CASE WHEN LTRIM(RTRIM(PhoneNumber)) = '' THEN 'N' ELSE 'Y' END, 
		EIN_Available = CASE WHEN (LTRIM(RTRIM(EIN)) = '' OR EIN is NULL ) THEN 'N' ELSE 'Y' END, 
		City_Available = CASE WHEN LTRIM(RTRIM(City)) = '' THEN 'N' ELSE 'Y' END, 
		Website_Available = CASE WHEN LTRIM(RTRIM(WebAddress)) = '' THEN 'N' ELSE 'Y' END,
		One_Per_Address_Company = LTRIM(RTRIM(NVL(ADDRESS,''))) + LTRIM(RTRIM(NVL(COMPANY,''))),
	   PRESENCE_OF_LATITUDE = CASE WHEN fLATITUDE <= 0 THEN 'N' ELSE 'Y' END,
	   PRESENCE_OF_LONGITUDE = CASE WHEN fLONGITUDE <= 0 THEN 'N' ELSE 'Y' END,
		Creditcardsaccepted = RTRIM(CREDITCARDSACCEPTED_1 + CREDITCARDSACCEPTED_2 + CREDITCARDSACCEPTED_3 + 
							  CREDITCARDSACCEPTED_4 + CREDITCARDSACCEPTED_5 + CREDITCARDSACCEPTED_6 +
							  CREDITCARDSACCEPTED_7 + CREDITCARDSACCEPTED_8 + CREDITCARDSACCEPTED_9 + 
							  CREDITCARDSACCEPTED_10 + CREDITCARDSACCEPTED_11 + CREDITCARDSACCEPTED_12 +
							  CREDITCARDSACCEPTED_13 + CREDITCARDSACCEPTED_14 + CREDITCARDSACCEPTED_15),
		Creditcardsaccepted_Multi =  ',' + ltrim(rtrim(CREDITCARDSACCEPTED_1)) + ',' +  ltrim(rtrim(CREDITCARDSACCEPTED_2)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_3)) + 
									 ',' + ltrim(rtrim(CREDITCARDSACCEPTED_4)) +',' +  ltrim(rtrim(CREDITCARDSACCEPTED_5)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_6)) +
									 ',' + ltrim(rtrim(CREDITCARDSACCEPTED_7)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_8)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_9)) + 
									 ',' + ltrim(rtrim(CREDITCARDSACCEPTED_10)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_11)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_12)) +
									 ',' + ltrim(rtrim(CREDITCARDSACCEPTED_13)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_14)) + ',' + ltrim(rtrim(CREDITCARDSACCEPTED_15))  + ',',
	
		PARENTABINUMBER_Back =  PARENTABINUMBER,
      TITLECODE =  TITLECODE_Back,
      ACL_BuyerCode =
         CASE WHEN ISNULL(ACL_buyercode1,'') <>''  THEN ',' + ACL_buyercode1 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode2,'') <>''  THEN ',' + ACL_buyercode2 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode3,'') <>''  THEN ',' + ACL_buyercode3 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode4,'') <>''  THEN ',' + ACL_buyercode4 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode5,'') <>''  THEN ',' + ACL_buyercode5 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode6,'') <>''  THEN ',' + ACL_buyercode6 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode7,'') <>''  THEN ',' + ACL_buyercode7 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode8,'') <>''  THEN ',' + ACL_buyercode8 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode9,'') <>''  THEN ',' + ACL_buyercode9 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode10,'') <>''  THEN ',' + ACL_buyercode10 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode11,'') <>''  THEN ',' + ACL_buyercode11 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode12,'') <>''  THEN ',' + ACL_buyercode12 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode13,'') <>''  THEN ',' + ACL_buyercode13 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode14,'') <>''  THEN ',' + ACL_buyercode14 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode15,'') <>''  THEN ',' + ACL_buyercode15 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode16,'') <>''  THEN ',' + ACL_buyercode16 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode17,'') <>''  THEN ',' + ACL_buyercode17 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode18,'') <>''  THEN ',' + ACL_buyercode18 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode19,'') <>''  THEN ',' + ACL_buyercode19 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode20,'') <>''  THEN ',' + ACL_buyercode20 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode21,'') <>''  THEN ',' + ACL_buyercode21 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode22,'') <>''  THEN ',' + ACL_buyercode22 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode23,'') <>''  THEN ',' + ACL_buyercode23 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode24,'') <>''  THEN ',' + ACL_buyercode24 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode25,'') <>''  THEN ',' + ACL_buyercode25 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode26,'') <>''  THEN ',' + ACL_buyercode26 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode27,'') <>''  THEN ',' + ACL_buyercode27 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode28,'') <>''  THEN ',' + ACL_buyercode28 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode29,'') <>''  THEN ',' + ACL_buyercode29 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode30,'') <>''  THEN ',' + ACL_buyercode30 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode31,'') <>''  THEN ',' + ACL_buyercode31 ELSE '' END +
         CASE WHEN ISNULL(ACL_buyercode32,'') <>''  THEN ',' + ACL_buyercode32 ELSE '' END,
      Vendor_ID ='',
      Geo_Match_Level='',
      Database_Flag ='',
      Work_At_Home_Indicator ='',
      Haspostal = 'Y';


Update tblBusinessIndividual 
set TITLECODE = CASE WHEN TITLECODE = '!'  THEN 'Z1'
   WHEN TITLECODE = '#'  THEN 'Z2'
   WHEN TITLECODE = '$'  THEN 'Z3'
   WHEN TITLECODE = '%'  THEN 'Z4'
   WHEN TITLECODE = '&'  THEN 'Z5'
   WHEN TITLECODE = '('  THEN 'Z6'
   WHEN TITLECODE = ')'  THEN 'Z7'
   WHEN TITLECODE = '+'  THEN 'Z8'
   WHEN TITLECODE = '-'  THEN 'Z9'
   WHEN TITLECODE = '.'  THEN 'Z10'
   WHEN TITLECODE = '/'  THEN 'Z11'
   WHEN TITLECODE = ':'  THEN 'Z12'
   WHEN TITLECODE = '='  THEN 'Z13'
   WHEN TITLECODE = '>'  THEN 'Z14'
   WHEN TITLECODE = '?'  THEN 'Z15'
   WHEN TITLECODE = '@'  THEN 'Z16'
   WHEN TITLECODE = '['  THEN 'Z17'
   WHEN TITLECODE = '\\'  THEN 'Z18'
   WHEN TITLECODE = ']'  THEN 'Z19'
   WHEN TITLECODE = '^'  THEN 'Z20'
   WHEN TITLECODE = '_'  THEN 'Z21'
   WHEN TITLECODE = '{'  THEN 'Z22'
   ELSE NVL(TITLECODE,'') END;           

Update tblBusinessIndividual 
set One_Per_Company_Name_Title = LEFT(LTRIM(RTRIM(NVL(Company,''))),30)+ LTRIM(RTRIM(NVL(TITLECODE,'')));