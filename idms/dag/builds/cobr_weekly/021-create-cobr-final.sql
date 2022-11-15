DROP TABLE IF EXISTS tblCobraWeekly881_Final;
CREATE TABLE tblCobraWeekly881_Final
(
   BUS_female_owned_business_LRFS          char(1),
   BUS_county_code_LRFS                    varchar(3),
   BUS_MSA_code_LRFS                       varchar(4),
   BUS_SIC_Code_6_LRFS                     varchar(6),
   BUS_secondary_SIC_code_1_LRFS           varchar(6),
   BUS_secondary_SIC_code_2_LRFS           varchar(6),
   BUS_secondary_SIC_code_3_LRFS           varchar(6),
   BUS_secondary_SIC_code_4_LRFS           varchar(6),
   Source_from_BUS_only_LRFS               varchar(6),
   Mail_Score_DPV_selection_LRFS           char(1),
   Home_Owner_Indicator_Percentage_LRFS    varchar(2),
   filler_4_LRFS                           varchar(4),
   SCF_LRFS                                varchar(3),
   Zip5_LRFS                               varchar(5),
   Age_Source_LRFS                         char(1),
   Age_Code_LRFS                           char(1),
   Religion_Code_LRFS                      char(1),
   Ethnicity_LRFS                          varchar(2),
   MatchCode_LRFS                          varchar(10),
   Sequence_Number_LRFS                    varchar(9),
   Address_Type_LRFS                       char(1),
   File_Code_LRFS                          char(1),
   DNC_Flag_DMA_phone_match_flag_LRFS      char(1),
   BUS_Fortune_1000_Code_LRFS              char(1),
   Date_CCYYMM_LRFS                        varchar(6),
   ADDRESS_INDICATOR_LRFS                  char(1),
   BUS_SIC_CODE_4_LRFS                     varchar(4),
   BUS_SIC_CODE_2_LRFS                     varchar(2),
   Phone2_Dashes_LRFS                      varchar(12),
   HOT_LIST_DATE_LRFS                      varchar(4),
   ZIP4_LRFS                               varchar(4),
   BUS_Company_name_LRFS                   varchar(30),
   BUS_Company_address_LRFS                varchar(57),
   BUS_Company_Fax_LRFS                    varchar(10),
   filler08_LRFS                           varchar(2),
   LEMS                                    varchar(18),
   PRES_OF_BUS_CONTACT_NAME                varchar(1),
   BUS_MATCH_FLAG                          varchar(1),
   SIC_Code_Client_2_LRFS                  varchar(2),
   ZIP5CRRT                                varchar(9),
   SIC_Code_Client_4_LRFS                  varchar(4),
   unique_matchcode_LRFS                   varchar(15),
   area_code_prefix_LRFS                   varchar(6),
   phone_suffix_LRFS                       varchar(4),
   phone_prefix_LRFS                       varchar(3),
   Area_Code_LRFS                          varchar(3),
   Mean_Home_Value_LRFS                    varchar(7),
   Vendor_ethnic_group_code_LRFS           char(1),
   Pres_of_BUS_contact_name_LRFS           char(1),
   Filler_3_LRFS                           varchar(2),
   BUS_contact_gender_code_LRFS            char(1),
   BUS_contact_title_code_LRFS             char(1),
   Vendor_ethnicity_code_LRFS              varchar(2),
   Pres_Phone_Flag_LRFS                    char(1),
   IOR_Code_LRFS                           char(1),
   Title                                   varchar(50),
   FirstName                               varchar(20),
   LastName                                varchar(20),
   DropFlag                                char(1),
   PermissionType                          char(1)  Default 'R',
   Federal_Do_Not_Call_New_LRFS            varchar(4),
   Vendor_religion_code_LRFS               char(1),
   vendor_language_code_LRFS               varchar(2),
   AddressLine1                            varchar(50),
   AddressLine2                            varchar(50),
   City                                    varchar(28),
   State                                   varchar(2),
   Marital_Status_LRFS                     char(1),
   BUS_location_sales_volume_LRFS          char(1),
   BUS_contact_last_name_LRFS              varchar(16),
   FullName                                varchar(50),
   BUS_year_started_CCYY_LRFS              varchar(4),
   File_type_LRFS                          char(1),
   Filler_LRFS                             varchar(2),
   Keycode_date_LRFS                       varchar(6),
   Zip                                     varchar(9),
   EmailAddress                            varchar(60),
   BUS_contact_middle_initial_LRFS         char(1),
   BUS_contact_first_name_LRFS             varchar(10),
   MFDU_Indicator_LRFS                     char(1),
   MSA_Code_LRFS                           varchar(4),
   ZipRadius                               char(1),
   CMiddle_Initial_LRFS                    char(1),
   Gender_Code_LRFS                        char(1),
   Company                                 varchar(50),
   County_Code_LRFS                        varchar(3),
   Date_CCYYMMDD_LRFS                      varchar(8),
   BUS_actual_location_employment_LRFS     varchar(5),
   BUS_location_employment_code_LRFS       char(1),
   Blank_Business_Address_Records_LRFS     char(1),
   CRRT_Code_LRFS                          varchar(4),
   BUS_business_in_motion_LRFS             varchar(2),
   DMA_Do_Not_Mail_FLag_LRFS               char(1),
   SBC__Clec_Flag_LRFS                     char(1),
   Home_Owner_Indicator_Code_LRFS          char(1),
   SIC_Code_Client_6_LRFS                  varchar(6),
   Chadd_Indicator_LRFS                    char(1),
   Source_Code_LRFS                        varchar(4),
   Federal_Do_Not_Call_Flag_LRFS           char(1),
   Federal_Do_Not_Call_Download_Date_LRFS  varchar(8),
   Deliverability_Code_LRFS                varchar(2),
   BUS_actual_location_sales_volume_LRFS   varchar(9),
   BUS_work_at_home_flag_LRFS              char(1),
   ListType                                char(1) Default 'Z',
   ProductCode                             char(2) Default '99',
   ListID                                  int default 10179,
   ID 					int identity unique, 
   Gender                                  char(1),
   Fax                                     char(20),
   Phone                                   char(20),
   INDIVIDUAL_MC                           varchar(17),
   Company_MC 							   VARCHAR(15),
   Pres_OF_Company                         char(1)
);

INSERT INTO tblCobraWeekly881_Final 
(
    County_Code_LRFS,
    Vendor_ethnicity_code_LRFS,
    CRRT_Code_LRFS,
    Area_Code_LRFS,
    Home_Owner_Indicator_Code_LRFS,
    BUS_year_started_CCYY_LRFS,
    Home_Owner_Indicator_Percentage_LRFS,
    State,
    BUS_contact_middle_initial_LRFS,
    AddressLine1,
    MFDU_Indicator_LRFS,
    Zip,
    Company,
    Age_Source_LRFS,
    BUS_female_owned_business_LRFS,
    CMiddle_Initial_LRFS,
    BUS_work_at_home_flag_LRFS,
    Chadd_Indicator_LRFS,
    Sequence_Number_LRFS,
    Age_Code_LRFS,
    BUS_Company_name_LRFS,
    City,
    SIC_Code_Client_6_LRFS,
    Phone,
    BUS_secondary_SIC_code_1_LRFS,
    filler_4_LRFS,
    Keycode_date_LRFS,
    BUS_Company_address_LRFS,
    SCF_LRFS,
    File_type_LRFS,
    DMA_Do_Not_Mail_FLag_LRFS,
    BUS_contact_last_name_LRFS,
    ZIP5CRRT,
    Federal_Do_Not_Call_Flag_LRFS,
    BUS_location_sales_volume_LRFS,
    BUS_secondary_SIC_code_4_LRFS,
    Address_Type_LRFS,
    LEMS,
    BUS_Company_Fax_LRFS,
    SIC_Code_Client_4_LRFS,
    BUS_contact_title_code_LRFS,
    BUS_actual_location_sales_volume_LRFS,
    LastName,
    BUS_secondary_SIC_code_2_LRFS,
    Marital_Status_LRFS,
    BUS_MATCH_FLAG,
    Deliverability_Code_LRFS,
    Blank_Business_Address_Records_LRFS,
    Mail_Score_DPV_selection_LRFS,
    DNC_Flag_DMA_phone_match_flag_LRFS,
    vendor_language_code_LRFS,
    Vendor_religion_code_LRFS,
    phone_suffix_LRFS,
    File_Code_LRFS,
    Federal_Do_Not_Call_Download_Date_LRFS,
    BUS_contact_first_name_LRFS,
    Ethnicity_LRFS,
    unique_matchcode_LRFS,
    Gender_Code_LRFS,
    phone_prefix_LRFS,
    BUS_contact_gender_code_LRFS,
    Phone2_Dashes_LRFS,
    BUS_SIC_CODE_2_LRFS,
    Title,
    BUS_SIC_Code_6_LRFS,
    Date_CCYYMMDD_LRFS,
    area_code_prefix_LRFS,
    ADDRESS_INDICATOR_LRFS,
    Date_CCYYMM_LRFS,
    MatchCode_LRFS,
    BUS_county_code_LRFS,
    BUS_secondary_SIC_code_3_LRFS,
    SIC_Code_Client_2_LRFS,
    Pres_of_BUS_contact_name_LRFS,
    Filler_LRFS,
    BUS_location_employment_code_LRFS,
    BUS_MSA_code_LRFS,
    AddressLine2,
    Source_from_BUS_only_LRFS,
    FirstName,
    BUS_SIC_CODE_4_LRFS,
    BUS_actual_location_employment_LRFS,
    SBC__Clec_Flag_LRFS,
    Zip5_LRFS,
    MSA_Code_LRFS,
    BUS_business_in_motion_LRFS,
    Mean_Home_Value_LRFS,
    BUS_Fortune_1000_Code_LRFS,
    Pres_Phone_Flag_LRFS,
    filler08_LRFS,
    HOT_LIST_DATE_LRFS,
    PRES_OF_BUS_CONTACT_NAME,
    ZIP4_LRFS,
    IOR_Code_LRFS,
    Vendor_ethnic_group_code_LRFS,
    Religion_Code_LRFS,
    Source_Code_LRFS,
    Filler_3_LRFS,
    FullName,
    Pres_OF_Company
)
SELECT 
	County_Code,
    Vendor_ethnicity_code,
    CRRT_Code,
    left(phone,3),
    Home_Owner_Indicator_Code,
    BUS_year_started_CCYY,
    Home_Owner_Indicator_Percentage,
    State,
    BUS_contact_middle_initial,
    AddressLine1,
    MFDU_Indicator,
    Zip,
    Company,
    Age_Source,
    BUS_female_owned_business,
    CMiddle_Initial,
    BUS_work_at_home_flag,
    Chadd_Indicator,
    Sequence_Number,
    Age_Code,
    BUS_Company_name,
    City,
    SIC_Code_Client_6,
    Phone,
    BUS_secondary_SIC_code_1,
    filler_4,
    Keycode_date,
    BUS_Company_address,
    left (zip,3),
    File_type,
    DMA_Do_Not_Mail_FLag,
    BUS_contact_last_name,
    LEFT(ZIP,5)+CRRT_CODE,
    Federal_Do_Not_Call_Flag,
    BUS_location_sales_volume,
    BUS_secondary_SIC_code_4,
    Address_Type,
    LEMS,
    BUS_Company_Fax,
    left(SIC_Code_Client_6,4),
    BUS_contact_title_code,
    BUS_actual_location_sales_volume,
    Lastname,
    BUS_secondary_SIC_code_2,
    Marital_Status,
    BUS_MATCH_FLAG,
    Deliverability_Code,
    Blank_Business_Address_Records,
    MailScoreDPVselection,
    DNC_Flag_DMA_phone_match_flag,
    vendor_language_code,
    Vendor_religion_code,
    SUBSTRING(Phone, 7, 4),
    File_Code,
    Federal_Do_Not_Call_Download_Date,
    BUS_contact_first_name,
    Ethnicity,
    LEFT(zip,5) + RTRIM(phone),
    Gender,
    SUBSTRING(Phone,4,3),
    BUS_contact_gender_code,
    left(Phone,3)+'-'+substring(Phone,4,3)+'-'+right(Phone,4),
    LEFT(BUS_SIC_CODE_6,2),
    Title,
    BUS_SIC_Code_6,
    Date_CCYYMMDD,
    left(Phone,6),
    CASE WHEN AddressLine2 = '' THEN '3' ELSE '4' END,
    left(Date_CCYYMMDD,6),
    MatchCode,
    BUS_county_code,
    BUS_secondary_SIC_code_3,
    left(SIC_Code_Client_6,2),
    Pres_of_BUS_contact_name,
    Filler,
    BUS_location_employment_code,
    BUS_MSA_code,
    AddressLine2,
    Source_from_BUS_only,
    Firstname,
    LEFT(BUS_SIC_CODE_6,4),
    BUS_actual_location_employment,
    SBC__Clec_Flag,
    left(zip,5),
    MSA_Code,
    BUS_business_in_motion,
    Mean_Home_Value,
    BUS_Fortune_1000_Code,
    Pres_Phone_Flag,
    filler08,
    SUBSTRING(DATE_CCYYMMDD,3,4),
    CASE WHEN BUS_contact_last_name = '' THEN 'N' ELSE 'Y' END,
    RIGHT(ZIP,4),
    IOR_Code,
    Vendor_ethnic_group_code,
    Religion_Code,
    Source_Code,
    Filler_3,
    Firstname + ' '+ Lastname,
    CASE WHEN company = '' THEN 'N' ELSE 'Y' END
From tblCobraWeekly881_Initial;

UPDATE tblCobraWeekly881_Final 
SET Individual_MC = GetMatchCode (NVL(REPLACE(FirstName ,'|',''),''), 
                                CASE WHEN (NVL(REPLACE(LastName ,'|',''),'') = '' or NVL(REPLACE(LastName ,'|',''),'') is NULL) THEN NVL(REPLACE(Company,'|',''),'') ELSE NVL(REPLACE(LastName ,'|',''),'') END, 
                                NVL(REPLACE(AddressLine1 ,'|',''),''), 
                                NVL(REPLACE(Zip ,'|',''),''), 
                                '' ,
                                'I'),
  Company_MC  = GetMatchCode (NVL(REPLACE(FirstName ,'|',''),''), 
                            NVL(REPLACE(LastName ,'|',''),''),
                            NVL(REPLACE(AddressLine1 ,'|',''),''),
                            NVL(REPLACE(Zip ,'|',''),''),
                            CASE WHEN (NVL(REPLACE(Company,'|',''),'') = '' or NVL(REPLACE(Company,'|',''),'') is NULL) THEN NVL(REPLACE(LastName ,'|',''),'') ELSE NVL(REPLACE(Company,'|',''),'') END,
                            'C');