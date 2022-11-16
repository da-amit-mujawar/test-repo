DROP TABLE IF EXISTS tmp_tblChild10_Lowest_Activitystatus;

SELECT MIN(Activitystatus) Activity_Status, Individual_ID
  INTO tmp_tblChild10_Lowest_Activitystatus
  FROM {wireless-tablename1}
 WHERE (
            Verified_Code = 'A'
            AND
            IndividualMatch IN ('1')
            AND
            Activitystatus BETWEEN 'A1' AND 'A7'
        )
 GROUP BY Individual_ID;

DROP TABLE IF EXISTS tmp_tblChild10_Lowest_Activitystatus_ID;

SELECT MIN(ID) ID
  INTO tmp_tblChild10_Lowest_Activitystatus_ID
  FROM {wireless-tablename1} a
 INNER JOIN tmp_tblChild10_Lowest_Activitystatus b
    ON a.Individual_ID = b.Individual_ID
   AND a.Activitystatus = b.Activity_Status
 WHERE (
            Verified_Code = 'A'
            AND
            IndividualMatch IN ('1')
            AND
            Activitystatus BETWEEN 'A1' AND 'A7'
        )
 GROUP BY a.Individual_ID;


-- HH Level
DROP TABLE IF EXISTS tmp_tblChild10_Lowest_Activitystatus_HH;

SELECT MIN(Activitystatus) Activity_Status, Company_ID
  INTO tmp_tblChild10_Lowest_Activitystatus_HH
  FROM {wireless-tablename1}
 WHERE (
            Verified_Code = 'A'
            AND
            IndividualMatch NOT IN ('1')
            AND
            Activitystatus BETWEEN 'A1' AND 'A7'
        )
    AND ID NOT IN (SELECT ID FROM tmp_tblChild10_Lowest_Activitystatus_ID)
 GROUP BY Company_ID;

DROP TABLE IF EXISTS tmp_tblChild10_Lowest_Activitystatus_ID_HH;

SELECT MIN(ID) ID
  INTO tmp_tblChild10_Lowest_Activitystatus_ID_HH
  FROM {wireless-tablename1} a
 INNER JOIN tmp_tblChild10_Lowest_Activitystatus_HH b
    ON a.Company_ID = b.Company_ID
   AND a.Activitystatus = b.Activity_Status
 WHERE (
            Verified_Code = 'A'
            AND
            IndividualMatch NOT IN ('1')
            AND
            Activitystatus BETWEEN 'A1' AND 'A7'
        )
 GROUP BY a.Company_ID;

DELETE FROM {wireless-tablename1}
 WHERE ID NOT IN (SELECT ID FROM tmp_tblChild10_Lowest_Activitystatus_ID
                   UNION
                  SELECT ID FROM tmp_tblChild10_Lowest_Activitystatus_ID_HH); 


DROP TABLE IF EXISTS tmp_tblChild10_Lowest_Activitystatus_HH;
DROP TABLE IF EXISTS tmp_tblChild10_Lowest_Activitystatus_ID_HH;

alter table {wireless-tablename1} add CellPhone varchar(10);

UPDATE {wireless-tablename1}
   SET CellPhone = CellPhone_Areacode + CellPhone_Number;

DELETE FROM {wireless-tablename1} WHERE Company_ID = 0 AND Individual_ID = 0;


--Drop temp tables
DROP TABLE IF EXISTS tmp_tblchild10_lowest_activitystatus;
DROP TABLE IF EXISTS tmp_tblchild10_lowest_activitystatus_id ;







