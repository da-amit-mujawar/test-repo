/*
call sp_suppress_oohb ('tblMain_20610_202011',1165, 'EMAILADDRESS', 'SHA256LOWER', 'CINCLUDE' , ' AND (B.dRecordDate > Getdate() -30 ) ' );

Reju Mathew 2021.1.26  Jira: Jira 626
created as SP on 2021.03.04

*/


CREATE OR REPLACE PROCEDURE sp_suppress_oohb (cTablename  VARCHAR(200), Databaseid int, cEmailfieldname VARCHAR(200), cSHA256fieldname VARCHAR(200), cSuppressionflagfieldname VARCHAR(200), cApplyfilterstring VARCHAR(500))
AS $$
DECLARE
  strDatabaseid VARCHAR(10);
  strSQL VARCHAR(max) ;
  intRowcount int;
BEGIN
  strDatabaseid  = CAST(Databaseid as VARCHAR(10));

  --1. Apply OO by email -- NOT List Specific suppressions
  strSQL = 'UPDATE {tblMain}
    SET {suppressionflagfieldname} =''O''
    FROM {tblMain}  A
    INNER JOIN EXCLUDE_OOHB_{Databaseid} B  ON UPPER(A.{emailfieldname} ) =  UPPER(B.EMAILADDRESS ) AND B.cFlag =''O'' AND B.LISTID = 0
    WHERE  (   A.{suppressionflagfieldname} IS NULL OR LTRIM(RTRIM(A.{suppressionflagfieldname}))='''' OR  LTRIM(RTRIM(UPPER(A.{suppressionflagfieldname}))) IN (''Y'',''M''))
    AND A.{emailfieldname}  <> ''''
    {Applyfilterstring} ; '  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  strSQL = replace(strSQL, '{emailfieldname}', cEmailfieldname);
  strSQL = replace(strSQL, '{sha256fieldname}', cSHA256fieldname);
  strSQL = replace(strSQL, '{suppressionflagfieldname}', cSuppressionflagfieldname);
  strSQL = replace(strSQL, '{Applyfilterstring}', cApplyfilterstring);

  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;

--2. Apply OO by email -- List Specific
strSQL = 'UPDATE {tblMain}
    SET {suppressionflagfieldname} =''O''
    FROM {tblMain}  A
    INNER JOIN EXCLUDE_OOHB_{Databaseid} B  ON UPPER(A.{emailfieldname} ) =  UPPER(B.EMAILADDRESS ) AND B.cFlag =''O'' AND B.LISTID = A.LISTID
    WHERE  (   A.{suppressionflagfieldname} IS NULL OR LTRIM(RTRIM(A.{suppressionflagfieldname}))='''' OR  LTRIM(RTRIM(UPPER(A.{suppressionflagfieldname}))) IN (''Y'',''M''))
    AND A.{emailfieldname}  <> ''''
   {Applyfilterstring} ; '  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  strSQL = replace(strSQL, '{emailfieldname}', cEmailfieldname);
  strSQL = replace(strSQL, '{sha256fieldname}', cSHA256fieldname);
  strSQL = replace(strSQL, '{suppressionflagfieldname}', cSuppressionflagfieldname);
  strSQL = replace(strSQL, '{Applyfilterstring}', cApplyfilterstring);
  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;

 --3. Apply HB by email
strSQL = 'UPDATE {tblMain}
    SET {suppressionflagfieldname} =''H''
    FROM {tblMain}  A
    INNER JOIN EXCLUDE_OOHB_{Databaseid} B  ON UPPER(A.{emailfieldname} ) =  UPPER(B.EMAILADDRESS ) AND B.cFlag =''H''
    WHERE  (   A.{suppressionflagfieldname} IS NULL OR LTRIM(RTRIM(A.{suppressionflagfieldname}))='''' OR  LTRIM(RTRIM(UPPER(A.{suppressionflagfieldname}))) IN (''Y'',''M''))
    AND A.{emailfieldname}  <> ''''
   {Applyfilterstring} ;'  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  strSQL = replace(strSQL, '{emailfieldname}', cEmailfieldname);
  strSQL = replace(strSQL, '{sha256fieldname}', cSHA256fieldname);
  strSQL = replace(strSQL, '{suppressionflagfieldname}', cSuppressionflagfieldname);
  strSQL = replace(strSQL, '{Applyfilterstring}', cApplyfilterstring);
  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;


--1. Apply OO by SHAR256Lower  -- NOT List Specific
strSQL = 'UPDATE {tblMain}
    SET {suppressionflagfieldname} =''O''
    FROM {tblMain}  A
    INNER JOIN EXCLUDE_OOHB_{Databaseid} B  ON UPPER(A.{sha256fieldname}) =  UPPER(B.SHA256LOWER ) AND B.cFlag =''O'' AND B.LISTID = 0
    WHERE  (   A.{suppressionflagfieldname} IS NULL OR LTRIM(RTRIM(A.{suppressionflagfieldname}))='''' OR  LTRIM(RTRIM(UPPER(A.{suppressionflagfieldname}))) IN (''Y'',''M''))
    AND A.{sha256fieldname} <> ''''
   {Applyfilterstring} ;'  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  strSQL = replace(strSQL, '{emailfieldname}', cEmailfieldname);
  strSQL = replace(strSQL, '{sha256fieldname}', cSHA256fieldname);
  strSQL = replace(strSQL, '{suppressionflagfieldname}', cSuppressionflagfieldname);
  strSQL = replace(strSQL, '{Applyfilterstring}', cApplyfilterstring);
  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;

--2. Apply OO by SHAR256Lower  -- List Specific
strSQL = 'UPDATE {tblMain}
    SET {suppressionflagfieldname} =''O''
    FROM {tblMain}  A
    INNER JOIN EXCLUDE_OOHB_{Databaseid} B  ON UPPER(A.{sha256fieldname}) =  UPPER(B.SHA256LOWER ) AND B.cFlag =''O'' AND B.LISTID = A.LISTID
    WHERE  (   A.{suppressionflagfieldname} IS NULL OR LTRIM(RTRIM(A.{suppressionflagfieldname}))='''' OR  LTRIM(RTRIM(UPPER(A.{suppressionflagfieldname}))) IN (''Y'',''M''))
    AND A.{sha256fieldname} <> ''''
   {Applyfilterstring} ;'  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  strSQL = replace(strSQL, '{emailfieldname}', cEmailfieldname);
  strSQL = replace(strSQL, '{sha256fieldname}', cSHA256fieldname);
  strSQL = replace(strSQL, '{suppressionflagfieldname}', cSuppressionflagfieldname);
  strSQL = replace(strSQL, '{Applyfilterstring}', cApplyfilterstring);
  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;

 --3. Apply HB by SHAR256Lower
 strSQL = 'UPDATE {tblMain}
    SET {suppressionflagfieldname} =''H''
    FROM {tblMain}  A
    INNER JOIN EXCLUDE_OOHB_{Databaseid} B  ON UPPER(A.{sha256fieldname}) =  UPPER(B.SHA256LOWER ) AND B.cFlag =''H''
    WHERE  (   A.{suppressionflagfieldname} IS NULL OR LTRIM(RTRIM(A.{suppressionflagfieldname}))='''' OR  LTRIM(RTRIM(UPPER(A.{suppressionflagfieldname}))) IN (''Y'',''M''))
    AND A.{sha256fieldname} <> ''''
   {Applyfilterstring} ;'  ;

  strSQL = replace(strSQL, '{tblMain}', cTablename);
  strSQL = replace(strSQL, '{Databaseid}', strDatabaseid);
  strSQL = replace(strSQL, '{emailfieldname}', cEmailfieldname);
  strSQL = replace(strSQL, '{sha256fieldname}', cSHA256fieldname);
  strSQL = replace(strSQL, '{suppressionflagfieldname}', cSuppressionflagfieldname);
  strSQL = replace(strSQL, '{Applyfilterstring}', cApplyfilterstring);
  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;


END;
$$ LANGUAGE plpgsql;
/




