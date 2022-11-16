DROP TABLE IF EXISTS NoSuchTable;


DROP TABLE IF EXISTS MailOrderBuyer;
CREATE TABLE MailOrderBuyer
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT 
  a.id AS id,
  MIN(mailorderbuyer_01.mailorderbuyer_01) AS mailorderbuyer_01,
  MIN(mailorderbuyer_02.mailorderbuyer_02) AS mailorderbuyer_02,
  MIN(mailorderbuyer_03.mailorderbuyer_03) AS mailorderbuyer_03,
  MIN(mailorderbuyer_04.mailorderbuyer_04) AS mailorderbuyer_04,
  MIN(mailorderbuyer_05.mailorderbuyer_05) AS mailorderbuyer_05,
  MIN(mailorderbuyer_06.mailorderbuyer_06) AS mailorderbuyer_06,
  MIN(mailorderbuyer_07.mailorderbuyer_07) AS mailorderbuyer_07,
  MIN(mailorderbuyer_08.mailorderbuyer_08) AS mailorderbuyer_08,
  MIN(mailorderbuyer_09.mailorderbuyer_09) AS mailorderbuyer_09,
  MIN(mailorderbuyer_10.mailorderbuyer_10) AS mailorderbuyer_10,
  MIN(mailorderbuyer_11.mailorderbuyer_11) AS mailorderbuyer_11,
  MIN(mailorderbuyer_12.mailorderbuyer_12) AS mailorderbuyer_12,
  MIN(mailorderbuyer_13.mailorderbuyer_13) AS mailorderbuyer_13,
  MIN(mailorderbuyer_14.mailorderbuyer_14) AS mailorderbuyer_14,
  MIN(mailorderbuyer_15.mailorderbuyer_15) AS mailorderbuyer_15,
  MIN(mailorderbuyer_16.mailorderbuyer_16) AS mailorderbuyer_16,
  MIN(mailorderbuyer_18.mailorderbuyer_18) AS mailorderbuyer_18
FROM
{new-load-table} a
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_01 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '01' GROUP BY familyid
  ) mailorderbuyer_01 
ON a.company_id = mailorderbuyer_01.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_02 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '02' GROUP BY familyid
  ) mailorderbuyer_02 
ON a.company_id = mailorderbuyer_02.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_03 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '03' GROUP BY familyid
  ) mailorderbuyer_03 
ON a.company_id = mailorderbuyer_03.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_04 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '04' GROUP BY familyid
  ) mailorderbuyer_04 
ON a.company_id = mailorderbuyer_04.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_05 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '05' GROUP BY familyid
  ) mailorderbuyer_05 
ON a.company_id = mailorderbuyer_05.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_06 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '06' GROUP BY familyid
  ) mailorderbuyer_06 
ON a.company_id = mailorderbuyer_06.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_07 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '07' GROUP BY familyid
  ) mailorderbuyer_07 
ON a.company_id = mailorderbuyer_07.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_08 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '08' GROUP BY familyid
  ) mailorderbuyer_08 
ON a.company_id = mailorderbuyer_08.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_09 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '09' GROUP BY familyid
  ) mailorderbuyer_09 
ON a.company_id = mailorderbuyer_09.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_10 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '10' GROUP BY familyid
  ) mailorderbuyer_10 
ON a.company_id = mailorderbuyer_10.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_11 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '11' GROUP BY familyid
  ) mailorderbuyer_11 
ON a.company_id = mailorderbuyer_11.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_12 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '12' GROUP BY familyid
  ) mailorderbuyer_12 
ON a.company_id = mailorderbuyer_12.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_13 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '13' GROUP BY familyid
  ) mailorderbuyer_13 
ON a.company_id = mailorderbuyer_13.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_14 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '14' GROUP BY familyid
  ) mailorderbuyer_14 
ON a.company_id = mailorderbuyer_14.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_15 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '15' GROUP BY familyid
  ) mailorderbuyer_15 
ON a.company_id = mailorderbuyer_15.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_16 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '16' GROUP BY familyid
  ) mailorderbuyer_16 
ON a.company_id = mailorderbuyer_16.familyid
LEFT JOIN 
(
  SELECT familyid, MIN(ccode) AS mailorderbuyer_18 FROM {MailOrderBuyer_ToBeDropped-load-table}
  WHERE ccode = '18' GROUP BY familyid
  ) mailorderbuyer_18 
ON a.company_id = mailorderbuyer_18.familyid
GROUP BY a.id;


