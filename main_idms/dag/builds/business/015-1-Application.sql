DROP TABLE IF EXISTS business_APPLICATION;
CREATE TABLE business_APPLICATION 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT 
	tblMain.id AS id,
	MIN(APPLICATION_A.APPLICATION_A) AS APPLICATION_A,
	MIN(APPLICATION_B.APPLICATION_B) AS APPLICATION_B,
	MIN(APPLICATION_C.APPLICATION_C) AS APPLICATION_C,
	MIN(APPLICATION_E.APPLICATION_E) AS APPLICATION_E,
	MIN(APPLICATION_F.APPLICATION_F) AS APPLICATION_F,
	MIN(APPLICATION_G.APPLICATION_G) AS APPLICATION_G,
	MIN(APPLICATION_H.APPLICATION_H) AS APPLICATION_H,
	MIN(APPLICATION_I.APPLICATION_I) AS APPLICATION_I,
	MIN(APPLICATION_J.APPLICATION_J) AS APPLICATION_J,
	MIN(APPLICATION_K.APPLICATION_K) AS APPLICATION_K,
	MIN(APPLICATION_L.APPLICATION_L) AS APPLICATION_L,
	MIN(APPLICATION_M.APPLICATION_M) AS APPLICATION_M,
	MIN(APPLICATION_N.APPLICATION_N) AS APPLICATION_N,
	MIN(APPLICATION_P.APPLICATION_P) AS APPLICATION_P
FROM tblBusinessIndividual tblMain
LEFT JOIN 
(
  SELECT AbiNumber,  MIN(cvalue) APPLICATION_a FROM Applicationcodes_992
  WHERE cvalue = 'A' GROUP BY AbiNumber
) APPLICATION_A 
ON APPLICATION_A.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_B FROM Applicationcodes_992
  WHERE cvalue = 'B' GROUP BY AbiNumber
) APPLICATION_B 
ON APPLICATION_B.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_C FROM Applicationcodes_992
  WHERE cvalue = 'C' GROUP BY AbiNumber
) APPLICATION_C 
ON APPLICATION_C.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_E FROM Applicationcodes_992
  WHERE cvalue = 'E' GROUP BY AbiNumber
) APPLICATION_E 
ON APPLICATION_E.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_F FROM Applicationcodes_992
  WHERE cvalue = 'F' GROUP BY AbiNumber
) APPLICATION_F 
ON APPLICATION_F.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_G FROM Applicationcodes_992
  WHERE cvalue = 'G' GROUP BY AbiNumber
) APPLICATION_G 
ON APPLICATION_G.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_H FROM Applicationcodes_992
  WHERE cvalue = 'H' GROUP BY AbiNumber
) APPLICATION_H 
ON APPLICATION_H.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_I FROM Applicationcodes_992
  WHERE cvalue = 'I' GROUP BY AbiNumber
) APPLICATION_I 
ON APPLICATION_I.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_J FROM Applicationcodes_992
  WHERE cvalue = 'J' GROUP BY AbiNumber
) APPLICATION_J 
ON APPLICATION_J.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_k FROM Applicationcodes_992
  WHERE cvalue = 'K' GROUP BY AbiNumber
) APPLICATION_K 
ON APPLICATION_K.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_l FROM Applicationcodes_992
  WHERE cvalue = 'L' GROUP BY AbiNumber
) APPLICATION_L 
ON APPLICATION_L.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_m FROM Applicationcodes_992
  WHERE cvalue = 'M' GROUP BY AbiNumber
) APPLICATION_M 
ON APPLICATION_M.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_n FROM Applicationcodes_992
  WHERE cvalue = 'N' GROUP BY AbiNumber
) APPLICATION_N 
ON APPLICATION_N.ABINumber = tblMain.AbiNumber
LEFT JOIN 
(
  SELECT AbiNumber AS AbiNumber,  MIN(cvalue) APPLICATION_p FROM Applicationcodes_992
  WHERE cvalue = 'P' GROUP BY AbiNumber
) APPLICATION_P 
ON APPLICATION_P.ABINumber = tblMain.AbiNumber
GROUP BY tblMain.id;