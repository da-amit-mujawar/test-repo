COPY {tablename1}
(Lems,
COMPANY_ID,
DonorMatch,
LTD_Months_Since_Last_Donation_Date,
LTD_Last_Donation_Date,
LTD_Number_Of_List_Sources,
LTD_Average_Donation_Per_Transaction,
M12_Number_Of_List_Sources,
M12_Average_Donation_Per_Transaction,
M12_CAT_AC_Number_Of_Donation,
M12_CAT_AN_Number_Of_Donation,
M12_CAT_CD_Number_Of_Donation,
M12_CAT_CG_Number_Of_Donation,
M12_CAT_CH_Number_Of_Donation,
M12_CAT_CO_Number_Of_Donation,
M12_CAT_CS_Number_Of_Donation,
M12_CAT_EA_Number_Of_Donation,
M12_CAT_ET_Number_Of_Donation,
M12_CAT_HG_Number_Of_Donation,
M12_CAT_HS_Number_Of_Donation,
M12_CAT_LI_Number_Of_Donation,
M12_CAT_NT_Number_Of_Donation,
M12_CAT_PA_Number_Of_Donation,
M12_CAT_RC_Number_Of_Donation,
M12_CAT_RD_Number_Of_Donation,
M12_CAT_RI_Number_Of_Donation,
M12_CAT_RJ_Number_Of_Donation,
M12_CAT_SS_Number_Of_Donation,
M12_CAT_SW_Number_Of_Donation,
M12_CAT_VE_Number_Of_Donation,
M48_Number_Of_Donation_Dollar_Cash,
M48_Number_Of_Donation_Dollar_Check,
M48_CAT_AN_Number_Of_Donation,
M48_CAT_AC_Number_Of_Donation,
M48_CAT_CD_Number_Of_Donation,
M48_CAT_CS_Number_Of_Donation,
M48_CAT_CG_Number_Of_Donation,
M48_CAT_CH_Number_Of_Donation,
M48_CAT_CO_Number_Of_Donation,
M48_CAT_EA_Number_Of_Donation,
M48_CAT_ET_Number_Of_Donation,
M48_CAT_HG_Number_Of_Donation,
M48_CAT_HS_Number_Of_Donation,
M48_CAT_RD_Number_Of_Donation,
M48_CAT_RI_Number_Of_Donation,
M48_CAT_LI_Number_Of_Donation,
M48_CAT_NT_Number_Of_Donation,
M48_CAT_PA_Number_Of_Donation,
M48_CAT_RC_Number_Of_Donation,
M48_CAT_RJ_Number_Of_Donation,
M48_CAT_SS_Number_Of_Donation,
M48_CAT_VE_Number_Of_Donation,
M48_CAT_SW_Number_Of_Donation,
M48_Number_Of_List_Sources,
M48_Total_Number_Of_Donations,
M48_Total_Dollar_Donations,
LTD_Number_Of_Donations_0_3_Months_Ago,
LTD_Number_Of_Donations_0_6_Months_Ago,
LTD_Number_Of_Donations_0_12_Months_Ago,
LTD_Number_Of_Donations_13_24_Months_Ago,
LTD_Number_Of_Donations_Over_24_Months_Ago,
LTD_LAST_DONATE_YYYYMM,
RowNbrAccNo)
FROM 's3://{s3-internal}{s3-key1}' 
iam_role '{iam}'
delimiter '|';

