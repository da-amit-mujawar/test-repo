UPDATE {costco_tblname1}
   SET	individual_id = CASE when individual_id_Varchar='' then 0 ELSE CAST (individual_id_Varchar  as BIGINT) END,
		ZipFull = zip+Zip4,
		FullName = ltrim(rtrim(FirstName)) + ' '+ ltrim(rtrim(LastName)),
		Listid =13195,
		ListType ='Z',
		PermissionType ='R',
		ProductCode = '',
		Gender ='',
		Has_Zip4 = CASE WHEN (LTRIM(RTRIM(Zip4))<>'' AND Zip4 is not null) THEN 'Y' ELSE 'N' END,
		Company_ID = CASE WHEN CE_Household_ID='' THEN 0 ELSE CAST (CE_Household_ID as BIGINT) END,
		SCF = LEFT(ZIP,3);


