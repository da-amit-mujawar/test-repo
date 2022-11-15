/*incident# 744706 [Active] DQI IND DQI3 additional fields  Reju M 2019.01.15*/
ALTER TABLE {tablename1} ADD DQI_I_YYYY_Of_Birth VARCHAR(4);
ALTER TABLE {tablename1} ADD DQI_I_MM_Of_Birth VARCHAR(2);

UPDATE {tablename1}
  SET DQI_I_YYYY_Of_Birth = Substring(DQI_I_YYYYMM_Of_Birth,1,4),
    DQI_I_MM_Of_Birth    = Substring(DQI_I_YYYYMM_Of_Birth,5,2)
;