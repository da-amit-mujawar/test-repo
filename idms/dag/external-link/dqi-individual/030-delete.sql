/* Delete Blank INDIVIDUAL_MC. Incident# 717686*/
DELETE FROM {tablename1} WHERE LTRIM(RTRIM(INDIVIDUAL_MC)) = '' OR INDIVIDUAL_MC IS NULL;