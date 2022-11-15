UPDATE tblBusinessIndividual_ctas1--61 min
SET COUNTYCODEBYSTATECODE = Census_State_Code_2010 + CountyCode,
    STATECODE = Census_State_Code_2010,
    STATE_COUNTY_CENSUSTRACT_BLOCK = CASE WHEN (Census_State_Code_2010  IS NULL OR LTRIM(RTRIM(Census_State_Code_2010 )) ='') THEN '  ' ELSE LTRIM(RTRIM(Census_State_Code_2010))  END
								   + CASE WHEN (COUNTYCODE IS NULL OR LTRIM(TRIM(COUNTYCODE)) ='') THEN '   ' ELSE LTRIM(RTRIM(COUNTYCODE)) END 
								   + CASE WHEN (CENSUSTRACT IS NULL OR LTRIM(RTRIM(CENSUSTRACT)) ='') THEN '      ' ELSE LTRIM(RTRIM(CENSUSTRACT)) END
								   + CASE WHEN (CENSUSBLOCK IS NULL OR LTRIM(RTRIM(CENSUSBLOCK)) ='') THEN ' ' ELSE LTRIM(RTRIM(CENSUSBLOCK)) END,
UNIQUEPROFESSIONALGUI = CASE WHEN PhoneNumber = '' THEN RIGHT(CAST(ID as varchar(20)),10) ELSE PhoneNumber END
;

/*
This is only used for CANADA
RegionCode_Multi = ',' +
          CASE WHEN RegionCode_01 <> '' THEN RegionCode_01 + ',' ELSE '' END + 
          CASE WHEN RegionCode_02 <> '' THEN RegionCode_02 + ',' ELSE '' END + 
          CASE WHEN RegionCode_03 <> '' THEN RegionCode_03 + ',' ELSE '' END + 
	      CASE WHEN RegionCode_04 <> '' THEN RegionCode_04 + ',' ELSE '' END + 
          CASE WHEN RegionCode_05 <> '' THEN RegionCode_05 + ',' ELSE '' END + 
          CASE WHEN RegionCode_06 <> '' THEN RegionCode_06 + ',' ELSE '' END +
		  CASE WHEN RegionCode_07 <> '' THEN RegionCode_07 + ',' ELSE '' END + 
          CASE WHEN RegionCode_08 <> '' THEN RegionCode_08 + ',' ELSE '' END + 
	      CASE WHEN RegionCode_09 <> '' THEN RegionCode_09 + ',' ELSE '' END + 
          CASE WHEN RegionCode_10 <> '' THEN RegionCode_10 + ',' ELSE '' END + 
          CASE WHEN RegionCode_11 <> '' THEN RegionCode_11 + ',' ELSE '' END +
		  CASE WHEN RegionCode_12 <> '' THEN RegionCode_12 + ',' ELSE '' END + 
          CASE WHEN RegionCode_13 <> '' THEN RegionCode_13 + ',' ELSE '' END + 
	      CASE WHEN RegionCode_14 <> '' THEN RegionCode_14 + ',' ELSE '' END + 
          CASE WHEN RegionCode_15 <> '' THEN RegionCode_15 + ',' ELSE '' END ;


UPDATE tblBusinessIndividual_ctas1
set RegionCode_Multi = CASE WHEN RegionCode_Multi =',' then '' else RegionCode_Multi end
*/

