/*
call sp_suppress_globaldomainemail ('tblMain_20610_202011',1165);

Reju Mathew 2021.1.26  Jira: Jira 626
created as SP on 2021.03.02

*/


CREATE OR REPLACE PROCEDURE sp_suppress_globaldomainemail (cTablename  VARCHAR(200), Databaseid int)
AS $$
DECLARE
  strDatabaseid VARCHAR(10);
  strSQL VARCHAR(max) ;
  intRowcount int;
BEGIN
  strDatabaseid  = CAST(Databaseid as VARCHAR(10));

  strSQL = 'UPDATE {tblMain}
			SET cInclude = ''G''
			FROM {tblMain}   A
			INNER JOIN EXCLUDE_SysOOGlobalDomain B
			ON UPPER(B.cDomain) = UPPER(A.Domain)
			WHERE (B.IDMSID=0  OR B.IDMSID ={Databaseid})
			AND A.cInclude in (''Y'',''M''); '  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  --RAISE INFO '1. strsql: %  ', strSQL;
  EXECUTE strSQL	;


  --Email suppress
  strSQL = 'UPDATE {tblMain}
			 SET cInclude = ''G''
			 FROM {tblMain}  A
			 INNER JOIN EXCLUDE_SysOOGlobalEmail  b
			 ON UPPER(b.cEmail) = UPPER(A.EmailAddress)
			 WHERE A.cInclude in (''Y'',''M''); '  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  --RAISE INFO '2.strsql: %  ', strSQL;
  EXECUTE strSQL	;


END;
$$ LANGUAGE plpgsql;
/




