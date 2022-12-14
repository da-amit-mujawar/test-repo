DROP TABLE IF EXISTS tblCobraWeekly881_Initial;
CREATE TABLE tblCobraWeekly881_Initial 
	(
		LastName VARCHAR(16),
		FirstName VARCHAR(10),
		CMiddle_Initial VARCHAR(1),
		Title VARCHAR(10),
		Company VARCHAR(30),
		AddressLine1 VARCHAR(30),
		City VARCHAR(16),
		State VARCHAR(2),
		Zip VARCHAR(9),
		Gender VARCHAR(1),
		File_type VARCHAR(1),
		Keycode_date VARCHAR(6),
		Filler VARCHAR(2),
		Blank_Business_Address_Records VARCHAR(1),
		CRRT_Code VARCHAR(4),
		Phone VARCHAR(10),
		Source_from_BUS_only VARCHAR(6),
		AddressLine2 VARCHAR(9),
		Date_CCYYMMDD VARCHAR(8),
		Mean_Home_Value VARCHAR(7),
		SBC__Clec_Flag VARCHAR(1),
		CON_NRANK_nielsen_ranking VARCHAR(1),
		Con_SHMDI_Deceased_Flag VARCHAR(1),
		DMA_Do_Not_Mail_FLag VARCHAR(1),
		County_Code VARCHAR(3),
		Deliverability_Code VARCHAR(2),
		Pres_Phone_Flag VARCHAR(1),
		File_Code VARCHAR(1),
		Address_Type VARCHAR(1),
		Sequence_Number VARCHAR(9),
		MatchCode VARCHAR(10),
		Ethnicity VARCHAR(2),
		Religion_Code VARCHAR(1),
		Age_Code VARCHAR(1),
		Age_Source VARCHAR(1),
		Marital_Status VARCHAR(1),
		MFDU_Indicator VARCHAR(1),
		MSA_Code VARCHAR(4),
		IOR_Code VARCHAR(1),
		Home_Owner_Indicator_Percentage VARCHAR(2),
		Home_Owner_Indicator_Code VARCHAR(1),
		SIC_Code_Client_6 VARCHAR(6),
		Chadd_Indicator VARCHAR(1),
		Source_Code VARCHAR(4),
		Federal_Do_Not_Call_Flag VARCHAR(1),
		Federal_Do_Not_Call_Download_Date VARCHAR(8),
		Federal_Do_Not_Call_New VARCHAR(4),
		DNC_Flag_DMA_phone_match_flag VARCHAR(1),
		LEMS1 VARCHAR(18),
		OLD_CONS_FILLER VARCHAR(41),
		MailScoreDPVselection VARCHAR(1),
		old_cons_filler2 VARCHAR(57),
		BUS_contact_last_name VARCHAR(16),
		BUS_contact_first_name VARCHAR(10),
		BUS_contact_middle_initial VARCHAR(1),
		Pres_of_BUS_contact_name VARCHAR(1),
		Filler_3 VARCHAR(2),
		BUS_contact_gender_code VARCHAR(1),
		BUS_contact_title_code VARCHAR(1),
		BUS_Fortune_1000_Code VARCHAR(1),
		BUS_year_started_CCYY VARCHAR(4),
		BUS_business_in_motion VARCHAR(2),
		BUS_female_owned_business VARCHAR(1),
		BUS_county_code VARCHAR(3),
		BUS_MSA_code VARCHAR(4),
		BUS_SIC_Code_6 VARCHAR(6),
		BUS_secondary_SIC_code_1 VARCHAR(6),
		BUS_secondary_SIC_code_2 VARCHAR(6),
		BUS_secondary_SIC_code_3 VARCHAR(6),
		BUS_secondary_SIC_code_4 VARCHAR(6),
		BUS_location_employment_code VARCHAR(1),
		BUS_actual_location_employment VARCHAR(5),
		BUS_location_sales_volume VARCHAR(1),
		BUS_actual_location_sales_volume VARCHAR(9),
		BUS_work_at_home_flag VARCHAR(1),
		Vendor_ethnicity_code VARCHAR(2),
		Vendor_religion_code VARCHAR(1),
		vendor_language_code VARCHAR(2),
		Vendor_ethnic_group_code VARCHAR(1),
		filler_4 VARCHAR(4),
		BUS_Company_name VARCHAR(30),
		BUS_Company_address VARCHAR(57),
		BUS_Company_Fax VARCHAR(10),
		filler08 VARCHAR(1),
		BUS_MATCH_FLAG VARCHAR(1),
		LEMS VARCHAR(18),
        filler_09 varchar(2)
);


copy tblCobraWeekly881_Initial
(
	LastName,
	FirstName,
	CMiddle_Initial,
	Title,
	Company,
	AddressLine1,
	City,
	State,
	Zip,
	Gender,
	File_type,
	Keycode_date,
	Filler,
	Blank_Business_Address_Records,
	CRRT_Code,
	Phone,
	Source_from_BUS_only,
	AddressLine2,
	Date_CCYYMMDD,
	Mean_Home_Value,
	SBC__Clec_Flag,
	CON_NRANK_nielsen_ranking,
	Con_SHMDI_Deceased_Flag,
	DMA_Do_Not_Mail_FLag,
	County_Code,
	Deliverability_Code,
	Pres_Phone_Flag,
	File_Code,
	Address_Type,
	Sequence_Number,
	MatchCode,
	Ethnicity,
	Religion_Code,
	Age_Code,
	Age_Source,
	Marital_Status,
	MFDU_Indicator,
	MSA_Code,
	IOR_Code,
	Home_Owner_Indicator_Percentage,
	Home_Owner_Indicator_Code,
	SIC_Code_Client_6,
	Chadd_Indicator,
	Source_Code,
	Federal_Do_Not_Call_Flag,
	Federal_Do_Not_Call_Download_Date,
	Federal_Do_Not_Call_New,
	DNC_Flag_DMA_phone_match_flag,
	LEMS1,
	OLD_CONS_FILLER,
	MailScoreDPVselection,
	old_cons_filler2,
	BUS_contact_last_name,
	BUS_contact_first_name,
	BUS_contact_middle_initial,
	Pres_of_BUS_contact_name,
	Filler_3,
	BUS_contact_gender_code,
	BUS_contact_title_code,
	BUS_Fortune_1000_Code,
	BUS_year_started_CCYY,
	BUS_business_in_motion,
	BUS_female_owned_business,
	BUS_county_code,
	BUS_MSA_code,
	BUS_SIC_Code_6,
	BUS_secondary_SIC_code_1,
	BUS_secondary_SIC_code_2,
	BUS_secondary_SIC_code_3,
	BUS_secondary_SIC_code_4,
	BUS_location_employment_code,
	BUS_actual_location_employment,
	BUS_location_sales_volume,
	BUS_actual_location_sales_volume,
	BUS_work_at_home_flag,
	Vendor_ethnicity_code,
	Vendor_religion_code,
	vendor_language_code,
	Vendor_ethnic_group_code,
	filler_4,
	BUS_Company_name,
	BUS_Company_address,
	BUS_Company_Fax,
	filler08,
	BUS_MATCH_FLAG,
	LEMS,
	FILLER_09
)
FROM 's3://{bucket_name}/{filename}' 
iam_role '{iam}'
fixedwidth
'LastName :16,
FirstName :10,
CMiddle_Initial :1,
Title :10,
Company :30,
AddressLine1 :30,
City :16,
State :2,
Zip :9,
Gender :1,
File_type :1,
Keycode_date :6,
Filler :2,
Blank_Business_Address_Records :1,
CRRT_Code :4,
Phone :10,
Source_from_BUS_only :6,
AddressLine2 :9,
Date_CCYYMMDD :8,
Mean_Home_Value :7,
SBC__Clec_Flag :1,
CON_NRANK_nielsen_ranking :1,
Con_SHMDI_Deceased_Flag :1,
DMA_Do_Not_Mail_FLag :1,
County_Code :3,
Deliverability_Code :2,
Pres_Phone_Flag :1,
File_Code :1,
Address_Type :1,
Sequence_Number :9,
MatchCode :10,
Ethnicity :2,
Religion_Code :1,
Age_Code :1,
Age_Source :1,
Marital_Status :1,
MFDU_Indicator :1,
MSA_Code :4,
IOR_Code :1,
Home_Owner_Indicator_Percentage :2,
Home_Owner_Indicator_Code :1,
SIC_Code_Client_6 :6,
Chadd_Indicator :1,
Source_Code :4,
Federal_Do_Not_Call_Flag :1,
Federal_Do_Not_Call_Download_Date :8,
Federal_Do_Not_Call_New :4,
DNC_Flag_DMA_phone_match_flag :1,
LEMS1 :18,
OLD_CONS_FILLER :41,
MailScoreDPVselection :1,
old_cons_filler2 :57,
BUS_contact_last_name :16,
BUS_contact_first_name :10,
BUS_contact_middle_initial :1,
Pres_of_BUS_contact_name :1,
Filler_3 :2,
BUS_contact_gender_code :1,
BUS_contact_title_code :1,
BUS_Fortune_1000_Code :1,
BUS_year_started_CCYY :4,
BUS_business_in_motion :2,
BUS_female_owned_business :1,
BUS_county_code :3,
BUS_MSA_code :4,
BUS_SIC_Code_6 :6,
BUS_secondary_SIC_code_1 :6,
BUS_secondary_SIC_code_2 :6,
BUS_secondary_SIC_code_3 :6,
BUS_secondary_SIC_code_4 :6,
BUS_location_employment_code :1,
BUS_actual_location_employment :5,
BUS_location_sales_volume :1,
BUS_actual_location_sales_volume :9,
BUS_work_at_home_flag :1,
Vendor_ethnicity_code :2,
Vendor_religion_code :1,
vendor_language_code :2,
Vendor_ethnic_group_code :1,
filler_4 :4,
BUS_Company_name :30,
BUS_Company_address :57,
BUS_Company_Fax :10,
filler08 :1,
BUS_MATCH_FLAG :1,
LEMS :18,
FILLER_09 :2';

UPDATE tblCobraWeekly881_Initial 
SET 
	LastName=UPPER(LastName),
	FirstName=UPPER(FirstName),
	CMiddle_Initial=UPPER(CMiddle_Initial),
	Title=UPPER(Title),
	Company=UPPER(Company),
	AddressLine1=UPPER(AddressLine1),
	City=UPPER(City),
	State=UPPER(State),
	Zip=UPPER(Zip),
	Gender=UPPER(Gender),
	File_type=UPPER(File_type),
	Keycode_date=UPPER(Keycode_date),
	Filler=UPPER(Filler),
	Blank_Business_Address_Records=UPPER(Blank_Business_Address_Records),
	CRRT_Code=UPPER(CRRT_Code),
	Phone=UPPER(Phone),
	Source_from_BUS_only=UPPER(Source_from_BUS_only),
	AddressLine2=UPPER(AddressLine2),
	Date_CCYYMMDD=UPPER(Date_CCYYMMDD),
	Mean_Home_Value=UPPER(Mean_Home_Value),
	SBC__Clec_Flag=UPPER(SBC__Clec_Flag),
	CON_NRANK_nielsen_ranking=UPPER(CON_NRANK_nielsen_ranking),
	Con_SHMDI_Deceased_Flag=UPPER(Con_SHMDI_Deceased_Flag),
	DMA_Do_Not_Mail_FLag=UPPER(DMA_Do_Not_Mail_FLag),
	County_Code=UPPER(County_Code),
	Deliverability_Code=UPPER(Deliverability_Code),
	Pres_Phone_Flag=UPPER(Pres_Phone_Flag),
	File_Code=UPPER(File_Code),
	Address_Type=UPPER(Address_Type),
	Sequence_Number=UPPER(Sequence_Number),
	MatchCode=UPPER(MatchCode),
	Ethnicity=UPPER(Ethnicity),
	Religion_Code=UPPER(Religion_Code),
	Age_Code=UPPER(Age_Code),
	Age_Source=UPPER(Age_Source),
	Marital_Status=UPPER(Marital_Status),
	MFDU_Indicator=UPPER(MFDU_Indicator),
	MSA_Code=UPPER(MSA_Code),
	IOR_Code=UPPER(IOR_Code),
	Home_Owner_Indicator_Percentage=UPPER(Home_Owner_Indicator_Percentage),
	Home_Owner_Indicator_Code=UPPER(Home_Owner_Indicator_Code),
	SIC_Code_Client_6=UPPER(SIC_Code_Client_6),
	Chadd_Indicator=UPPER(Chadd_Indicator),
	Source_Code=UPPER(Source_Code),
	Federal_Do_Not_Call_Flag=UPPER(Federal_Do_Not_Call_Flag),
	Federal_Do_Not_Call_Download_Date=UPPER(Federal_Do_Not_Call_Download_Date),
	Federal_Do_Not_Call_New=UPPER(Federal_Do_Not_Call_New),
	DNC_Flag_DMA_phone_match_flag=UPPER(DNC_Flag_DMA_phone_match_flag),
	LEMS1=UPPER(LEMS1),
	OLD_CONS_FILLER=UPPER(OLD_CONS_FILLER),
	MailScoreDPVselection=UPPER(MailScoreDPVselection),
	old_cons_filler2=UPPER(old_cons_filler2),
	BUS_contact_last_name=UPPER(BUS_contact_last_name),
	BUS_contact_first_name=UPPER(BUS_contact_first_name),
	BUS_contact_middle_initial=UPPER(BUS_contact_middle_initial),
	Pres_of_BUS_contact_name=UPPER(Pres_of_BUS_contact_name),
	Filler_3=UPPER(Filler_3),
	BUS_contact_gender_code=UPPER(BUS_contact_gender_code),
	BUS_contact_title_code=UPPER(BUS_contact_title_code),
	BUS_Fortune_1000_Code=UPPER(BUS_Fortune_1000_Code),
	BUS_year_started_CCYY=UPPER(BUS_year_started_CCYY),
	BUS_business_in_motion=UPPER(BUS_business_in_motion),
	BUS_female_owned_business=UPPER(BUS_female_owned_business),
	BUS_county_code=UPPER(BUS_county_code),
	BUS_MSA_code=UPPER(BUS_MSA_code),
	BUS_SIC_Code_6=UPPER(BUS_SIC_Code_6),
	BUS_secondary_SIC_code_1=UPPER(BUS_secondary_SIC_code_1),
	BUS_secondary_SIC_code_2=UPPER(BUS_secondary_SIC_code_2),
	BUS_secondary_SIC_code_3=UPPER(BUS_secondary_SIC_code_3),
	BUS_secondary_SIC_code_4=UPPER(BUS_secondary_SIC_code_4),
	BUS_location_employment_code=UPPER(BUS_location_employment_code),
	BUS_actual_location_employment=UPPER(BUS_actual_location_employment),
	BUS_location_sales_volume=UPPER(BUS_location_sales_volume),
	BUS_actual_location_sales_volume=UPPER(BUS_actual_location_sales_volume),
	BUS_work_at_home_flag=UPPER(BUS_work_at_home_flag),
	Vendor_ethnicity_code=UPPER(Vendor_ethnicity_code),
	Vendor_religion_code=UPPER(Vendor_religion_code),
	vendor_language_code=UPPER(vendor_language_code),
	Vendor_ethnic_group_code=UPPER(Vendor_ethnic_group_code),
	filler_4=UPPER(filler_4),
	BUS_Company_name=UPPER(BUS_Company_name),
	BUS_Company_address=UPPER(BUS_Company_address),
	BUS_Company_Fax=UPPER(BUS_Company_Fax),
	filler08=UPPER(filler08),
	BUS_MATCH_FLAG=UPPER(BUS_MATCH_FLAG),
	LEMS=UPPER(LEMS);