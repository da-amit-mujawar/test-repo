DROP TABLE IF EXISTS {tablename1};

CREATE TABLE {tablename1} ( 
INDIVIDUAL_MC varchar(17)  PRIMARY KEY encode raw,
DQI_I_INDIVIDUALID varchar(12) encode zstd,
DQI_I_FAMILYID varchar(12) encode zstd,
DQI_I_Age varchar(3) encode bytedict,
DQI_I_Gender char(1) encode zstd,
DQI_I_Head_of_Household_Flag char(1) encode zstd,
DQI_I_Parent_Flag char(1) encode zstd,
DQI_I_Political_Affiliation_Code char(1) encode zstd,
DQI_I_Spouse_Flag char(1) encode zstd,
DQI_I_Year_Birth_Range char(1) encode zstd,  
DQI_I_YYYYMMDD_Of_Birth varchar(8) encode zstd,
DQI_I_YYYYMM_Of_Birth varchar(6) encode zstd,
DQI_I_Ethnic_Code varchar(2) encode zstd,
DQI_I_Religion_Code char(1) encode zstd,
DQI_I_Group_Code char(1) encode zstd,
DQI_I_Spoken_Language varchar(2) encode zstd,
DQI_I_Country_Origin varchar(2) encode zstd,
DQI_I_Assimilation_Code char(1) encode zstd,
DQI_I_Marital_status_code char(1) encode zstd,
DQI_I_Grandpatent_flag char(1) encode zstd,
DQI_I_Infopersona_Cluster varchar(2) encode bytedict,
DQI_I_Infopersona_SuperCluster char(1) encode zstd,
DQI_I_DigitalMatchCode varchar(1) encode zstd,
DQI_I_New_mover_Flag varchar(1) encode zstd,
DQI_I_New_mover_Date varchar(5) encode zstd,
DQI_I_County_Code varchar(3) encode bytedict)
DISTSTYLE KEY
DISTKEY(INDIVIDUAL_MC)
SORTKEY(INDIVIDUAL_MC);