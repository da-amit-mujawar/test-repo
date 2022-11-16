DROP TABLE IF EXISTS {tablename1}; 

CREATE TABLE {tablename1}
(
id int identity encode az64,
Lems  VARCHAR(18)  PRIMARY KEY encode raw,
Matchcode  VARCHAR(25) encode zstd,
exp_id  VARCHAR(10) encode zstd,
experian_income  VARCHAR(1) encode zstd,
experian_enhanced_income  VARCHAR(1) encode zstd,
experian_person1_DOB  VARCHAR(6) encode zstd,
experian_estimated_age  VARCHAR(2) encode zstd,
experian_exact_age  VARCHAR(2) encode bytedict,
experian_presence_of_child_0_18  VARCHAR(2) encode zstd,
experian_Ethnic_Ethnic  VARCHAR(2) encode zstd,
experian_Ethnic_Experian_Group  VARCHAR(2) encode zstd,
experian_Ethnic_Religion  VARCHAR(1) encode zstd,
experian_Ethnic_Language_Preference  VARCHAR(2) encode zstd,
experian_Ethnic_Group  VARCHAR(1) encode zstd,
experian_Ethnic_Country_Of_Origin  VARCHAR(2) encode zstd,
Experian_Donor_any  VARCHAR(1) encode zstd,
Experian_Child_Any  VARCHAR(1) encode zstd,
Experian_Child_Array varchar(75) encode zstd,
Experian_Donor_Array varchar(75) encode zstd,
Experian_LifeStyle_Array varchar(250) encode zstd,
Experian_income_Array varchar(40) encode zstd,
Experian_age_Array varchar(10) encode bytedict
)
DISTSTYLE KEY
DISTKEY(lems)
SORTKEY(LEMS);



