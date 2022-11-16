COPY {tablename1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
fixedwidth 
'INDIVIDUAL_MC:17,
DQI_I_INDIVIDUALID:12,
DQI_I_FAMILYID:12,
DQI_I_Age:3,
DQI_I_Gender:1,
DQI_I_Head_of_Household_Flag:1,
DQI_I_Parent_Flag:1,
DQI_I_Political_Affiliation_Code:1,
DQI_I_Spouse_Flag:1,
DQI_I_Year_Birth_Range:1,  
DQI_I_YYYYMMDD_Of_Birth:8,
DQI_I_YYYYMM_Of_Birth:6,
DQI_I_Ethnic_Code:2,
DQI_I_Religion_Code:1,
DQI_I_Group_Code:1,
DQI_I_Spoken_Language:2,
DQI_I_Country_Origin:2,
DQI_I_Assimilation_Code:1,
DQI_I_Marital_status_code:1,
DQI_I_Grandpatent_flag:1,
DQI_I_Infopersona_Cluster:2,
DQI_I_Infopersona_SuperCluster:1,
DQI_I_DigitalMatchCode:1,
DQI_I_New_mover_Flag:1,
DQI_I_New_mover_Date:5,
DQI_I_County_Code:3';