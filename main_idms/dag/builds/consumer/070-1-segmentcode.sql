DROP TABLE IF EXISTS NoSuchTable;


DROP TABLE IF EXISTS SEGMENTCODE;
CREATE TABLE SEGMENTCODE 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT 
  a.id AS id,
  MIN(SEGMENTCODE_A.segmentcode_a) AS segmentcode_a,
  MIN(SEGMENTCODE_F.segmentcode_f) AS segmentcode_f,
  MIN(SEGMENTCODE_E.segmentcode_e) AS segmentcode_e,
  MIN(SEGMENTCODE_I.segmentcode_i) AS segmentcode_i,
  MIN(SEGMENTCODE_G.segmentcode_g) AS segmentcode_g,
  MIN(SEGMENTCODE_H.segmentcode_h) AS segmentcode_h,
  MIN(SEGMENTCODE_J.segmentcode_j) AS segmentcode_j

FROM
{new-load-table} a
LEFT JOIN 
(
  SELECT individual_id AS individual_id,  MIN(cvalue) segmentcode_a FROM {segcode-load-table}
  WHERE cvalue = 'A' GROUP BY individual_id
) SEGMENTCODE_A 
ON a.individual_id = SEGMENTCODE_A.individual_id
LEFT JOIN 
(
  SELECT individual_id AS individual_id, MIN(cvalue) segmentcode_e FROM {segcode-load-table}
  WHERE  cvalue = 'E' GROUP BY individual_id
  ) SEGMENTCODE_E 
ON a.individual_id = SEGMENTCODE_E.individual_id
LEFT JOIN 
(
  SELECT individual_id AS individual_id, MIN(cvalue) segmentcode_f FROM {segcode-load-table}
  WHERE cvalue = 'F' GROUP BY individual_id
  ) SEGMENTCODE_F 
ON a.individual_id = SEGMENTCODE_F.individual_id
LEFT JOIN 
(
  SELECT individual_id AS individual_id, MIN(cvalue) segmentcode_g FROM {segcode-load-table}
  WHERE cvalue = 'G' GROUP BY individual_id
  ) SEGMENTCODE_G 
ON a.individual_id = SEGMENTCODE_G.individual_id
LEFT JOIN 
(
  SELECT individual_id AS individual_id, MIN(cvalue) segmentcode_h FROM {segcode-load-table} 
  WHERE cvalue = 'H' GROUP BY individual_id
  ) SEGMENTCODE_H 
ON a.individual_id = SEGMENTCODE_H.individual_id
LEFT JOIN 
(
  SELECT individual_id AS individual_id, MIN(cvalue) segmentcode_i FROM {segcode-load-table}
  WHERE cvalue = 'I' GROUP BY individual_id
  ) SEGMENTCODE_I 
ON a.individual_id = SEGMENTCODE_I.individual_id
LEFT JOIN 
(
  SELECT individual_id AS individual_id, MIN(cvalue) segmentcode_j FROM {segcode-load-table}
  WHERE cvalue = 'J' GROUP BY individual_id
  ) SEGMENTCODE_J 
ON a.individual_id = SEGMENTCODE_J.individual_id
GROUP BY a.id;


