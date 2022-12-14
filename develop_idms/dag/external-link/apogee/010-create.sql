DROP TABLE IF EXISTS {tablename1}; 

CREATE TABLE {tablename1}
(
Lems  VARCHAR(18) PRIMARY KEY encode zstd,
COMPANY_ID	bigint encode zstd,
DonorMatch	char(1) encode zstd,
LTD_Months_Since_Last_Donation_Date	integer encode az64,
LTD_Last_Donation_Date	char(8) encode zstd,
LTD_Number_Of_List_Sources	integer encode zstd,
LTD_Average_Donation_Per_Transaction integer encode zstd,	
M12_Number_Of_List_Sources	integer encode zstd,
M12_Average_Donation_Per_Transaction integer encode zstd,	
M12_CAT_AC_Number_Of_Donation	integer encode zstd,
M12_CAT_AN_Number_Of_Donation	integer encode zstd,
M12_CAT_CD_Number_Of_Donation	integer encode zstd,
M12_CAT_CG_Number_Of_Donation	integer encode zstd,
M12_CAT_CH_Number_Of_Donation	integer encode zstd,
M12_CAT_CO_Number_Of_Donation	integer encode zstd,
M12_CAT_CS_Number_Of_Donation	integer encode zstd,
M12_CAT_EA_Number_Of_Donation	integer encode zstd,
M12_CAT_ET_Number_Of_Donation	integer encode zstd,
M12_CAT_HG_Number_Of_Donation	integer encode zstd,
M12_CAT_HS_Number_Of_Donation	integer encode zstd,
M12_CAT_LI_Number_Of_Donation	integer encode zstd,
M12_CAT_NT_Number_Of_Donation	integer encode zstd,
M12_CAT_PA_Number_Of_Donation	integer encode zstd,
M12_CAT_RC_Number_Of_Donation	integer encode zstd,
M12_CAT_RD_Number_Of_Donation	integer encode zstd,
M12_CAT_RI_Number_Of_Donation	integer encode zstd,
M12_CAT_RJ_Number_Of_Donation	integer encode zstd,
M12_CAT_SS_Number_Of_Donation	integer encode zstd,
M12_CAT_SW_Number_Of_Donation	integer encode az64,
M12_CAT_VE_Number_Of_Donation	integer encode zstd,
M48_Number_Of_Donation_Dollar_Cash	integer encode zstd,
M48_Number_Of_Donation_Dollar_Check	integer encode zstd,
M48_CAT_AN_Number_Of_Donation	integer encode zstd,
M48_CAT_AC_Number_Of_Donation	integer encode zstd,
M48_CAT_CD_Number_Of_Donation	integer encode zstd,
M48_CAT_CS_Number_Of_Donation	integer encode zstd,
M48_CAT_CG_Number_Of_Donation	integer encode zstd,
M48_CAT_CH_Number_Of_Donation	integer encode zstd,
M48_CAT_CO_Number_Of_Donation	integer encode zstd,
M48_CAT_EA_Number_Of_Donation	integer encode zstd,
M48_CAT_ET_Number_Of_Donation	integer encode zstd,
M48_CAT_HG_Number_Of_Donation	integer encode zstd,
M48_CAT_HS_Number_Of_Donation	integer encode zstd,
M48_CAT_RD_Number_Of_Donation	integer encode zstd,
M48_CAT_RI_Number_Of_Donation	integer encode zstd,
M48_CAT_LI_Number_Of_Donation	integer encode zstd,
M48_CAT_NT_Number_Of_Donation	integer encode zstd,
M48_CAT_PA_Number_Of_Donation	integer encode zstd,
M48_CAT_RC_Number_Of_Donation	integer encode zstd,
M48_CAT_RJ_Number_Of_Donation	integer encode zstd,
M48_CAT_SS_Number_Of_Donation	integer encode zstd,
M48_CAT_VE_Number_Of_Donation	integer encode zstd,
M48_CAT_SW_Number_Of_Donation	integer encode az64,
M48_Number_Of_List_Sources	integer encode zstd,
M48_Total_Number_Of_Donations	integer encode zstd,
M48_Total_Dollar_Donations	integer encode zstd,
LTD_Number_Of_Donations_0_3_Months_Ago	integer encode zstd,
LTD_Number_Of_Donations_0_6_Months_Ago	integer encode zstd,
LTD_Number_Of_Donations_0_12_Months_Ago	integer encode zstd,
LTD_Number_Of_Donations_13_24_Months_Ago	integer encode zstd,
LTD_Number_Of_Donations_Over_24_Months_Ago	integer encode zstd,
LTD_LAST_DONATE_YYYYMM	char(6) encode bytedict,
RowNbrAccNo	integer encode az64)

DISTSTYLE KEY
DISTKEY(LEMS)
SORTKEY(LEMS);
