CREATE OR REPLACE PROCEDURE sp_AOPScript_B2C (Databaseid IN int, cTablename  IN VARCHAR(200), boolEnableDebug IN int)
AS $$
DECLARE
	strSQL1 VARCHAR(max);
    column_rec RECORD;
BEGIN
  
  SELECT INTO column_rec * FROM information_schema.columns WHERE LOWER(table_name) = LOWER(cTablename) AND LOWER(column_name) = 'sqlmc';
  IF FOUND THEN
    strSQL1 = 'UPDATE ' + cTablename  + ' SET SQLMC = udf_sim_enhanced_matchcode2(FirstName, LastName, AddressLine1, Zip, Company);';

    IF boolEnableDebug = 1 THEN
      RAISE INFO '%', strSQL1;
    END IF;
    EXECUTE strSQL1;
  END IF;    
END;
$$ LANGUAGE plpgsql;
