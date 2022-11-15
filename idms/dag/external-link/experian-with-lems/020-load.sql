COPY {tablename1}
(
Lems,
Matchcode,
EXP_id,
experian_income,
experian_enhanced_income,
experian_person1_DOB,
experian_estimated_age,
experian_exact_age,
experian_presence_of_child_0_18,
experian_Ethnic_Ethnic,
experian_Ethnic_Experian_Group,
experian_Ethnic_Religion,
experian_Ethnic_Language_Preference,
experian_Ethnic_Group  ,
experian_Ethnic_Country_Of_Origin,
Experian_Donor_any,
Experian_Child_Any,
Experian_Child_Array,
Experian_Donor_Array,
Experian_LifeStyle_Array,
Experian_income_Array,
Experian_age_Array 
)
FROM 's3://{s3-internal}{s3-key1}' 
iam_role '{iam}'
delimiter '|';


ALTER TABLE {tablename1} ADD Experian_Age_Combined  INT;
UPDATE {tablename1}
  SET Experian_age_Combined =  
        CASE WHEN (LTRIM(RTRIM(experian_exact_age)) <>'' AND experian_exact_age IS NOT NULL ) THEN CAST (experian_exact_age AS INT)
            WHEN (LTRIM(RTRIM(experian_estimated_age)) <>'' AND experian_estimated_age IS NOT NULL ) THEN CAST (experian_estimated_age AS INT)
        END
		;
		


