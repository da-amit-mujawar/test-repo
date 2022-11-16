/* these are for changing the Company_ID and Individual_ID field data type */

ALTER TABLE {tablename1} RENAME Company_ID TO Company_ID_Varchar;
ALTER TABLE {tablename1} RENAME Individual_ID TO Individual_ID_Varchar;
ALTER TABLE {tablename1} ADD Company_ID BIGINT DEFAULT 0;
ALTER TABLE {tablename1} ADD Individual_ID BIGINT DEFAULT 0;


UPDATE {tablename1}
SET  Company_ID = CAST(Company_ID_Varchar AS BIGINT)
WHERE Company_ID_Varchar<>'';


UPDATE {tablename1}
SET  Individual_ID = CAST(Individual_ID_Varchar AS BIGINT)
WHERE Individual_ID_Varchar<>'';


