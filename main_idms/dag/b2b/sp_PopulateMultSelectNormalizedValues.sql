
/*
Jira 881

convert usp_PopulateMultSelectNormalizedValues as SQL script
--python Dictionary method using udf_getdecodesfromdictionaryarray_multi

--call sp_PopulateMultSelectNormalizedValues(1008, 12126 , 3710089,339766,'Consumer_DB_Product_Category', 'PROD_ARRAY_ALL_RAW','tblMain_21207_202110_rm_tobedropped', 1);
--call sp_PopulateMultSelectNormalizedValues(1008, 12126 , 3710089,339766,'Consumer_DB_Product_Category', 'PROD_ARRAY_ALL_RAW','tblMain_21207_202110', 1);

--added to github.  some testing codes are here.
--K:\List Updates - IDMS\AWS Build Migration\Redshift_StoredProcedures

--last parameter for boolEnableDebug. 0 for prod

Reju Mathew 2021.02.17
*/

CREATE OR REPLACE PROCEDURE sp_PopulateMultSelectNormalizedValues (
    Databaseid IN int,
    Listid int,
    BuildTableLayoutID IN int,
    BuildLoLID IN int,
    NormalizedFieldname IN VARCHAR(max),
    ToBeNormalizedFieldname IN VARCHAR(max),
    cTablename  IN VARCHAR(200),
    boolEnableDebug IN int
)
AS $$
DECLARE
  cBuildID VARCHAR(10);
  cDatabaseID VARCHAR(20);
  cListid VARCHAR(20);
  cBuildTableLayoutID VARCHAR(20);
  cBuildLoLID VARCHAR(20);
  cTableName_DD VARCHAR (200);
  strSQL VARCHAR(max) ;
  dictValues VARCHAR(max) ;
  cTableName_DD_rollup  VARCHAR (200);
  cFieldToUpdate VARCHAR(200);
  intRowcount int;
  intBuildID int;

BEGIN

  cBuildID = split_part (cTablename,'_',2);
  cDatabaseID = CAST (Databaseid AS VARCHAR(20));
  cListid = CAST (Listid AS VARCHAR (20) );
  cBuildTableLayoutID = CAST (BuildTableLayoutID AS VARCHAR (20));
  cBuildLoLID = CAST(BuildLoLID AS VARCHAR (20));

  cFieldToUpdate  = NormalizedFieldname;
  cTableName_DD = 'public.tmpSplitMultiNormalized_' + cDatabaseID +'_' + cListid + '_DD_tobedropped';
  cTableName_DD_rollup = 'public.tmpSplitMultiNormalized_' + cDatabaseID +'_' + cListid + '_DD_Rollup_tobedropped';


  strSQL ='DROP TABLE IF EXISTS '+ cTableName_DD + ';';
  EXECUTE strSQL	;

  strSQL =' CREATE TABLE ' + cTableName_DD  + '
  (   Listid int,
      cFieldName VARCHAR(200),
      cNormFieldName VARCHAR(200),
      cValue VARCHAR(max),
      cNormalizedValue VARCHAR(max),
      cDictValue VARCHAR(max)
  )
  DISTSTYLE ALL
  SORTKEY (Listid, cFieldName, cNormFieldName,cValue,cNormalizedValue);' ;

  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;

  IF boolEnableDebug = 1 THEN
    RAISE INFO 'Test Variables: 1: %, 2: %,  3: %, 4: %,  5: %  ', cTableName_DD,NormalizedFieldname , ToBeNormalizedFieldname,cBuildTableLayoutID, cBuildLoLID;
  END IF;

--remove double quote, comma and : from the dd and normalized code
strSQL =' INSERT INTO ' + cTableName_DD +  ' (Listid,cFieldName,cNormFieldName, cValue, cNormalizedValue,cDictValue)
      SELECT ' + cListid + ', ''' + NormalizedFieldname + ''', ''' + ToBeNormalizedFieldname + ''', BuildDD.cValue, BuildDD.cNormalizedValue,

      ''"''
      +   REPLACE(REPLACE(REPLACE (TRIM(BuildDD.cValue),'':'',''''),''"'',''''),'','','''')
      +''":"''
      +   REPLACE(REPLACE(REPLACE (TRIM(BuildDD.cNormalizedValue),'':'',''''),''"'',''''),'','','''')
      +''"''   as cDictValue

    	from public.sql_tblBuildDD BuildDD
    	Where BuildTableLayoutID  = '+ cBuildTableLayoutID +'
    	AND BuildLoLID = '+ cBuildLoLID +' ;' ;

  --RAISE INFO 'strsql: %  ', strSQL;
  EXECUTE strSQL	;

  strSQL = 'SELECT  count(*) from '+  cTableName_DD +';' ;
  EXECUTE strSQL INTO intRowcount;
  IF boolEnableDebug = 1 THEN RAISE INFO 'Database ID: %,  BuildID: %,   No. Records in % : %', cDatabaseId, cBuildID, cTableName_DD, intRowcount; END IF;

  --dont proceed if count is 0. means no fields to normalize.
  IF intRowcount = 0 THEN
    BEGIN
       strSQL ='DROP TABLE IF EXISTS '+ cTableName_DD + ';';
       EXECUTE strSQL	;
       RETURN;
     END;
  END IF;


 -- Now rollup the value
  strSQL = ' DROP TABLE IF EXISTS ' + cTableName_DD_rollup + ';';
  EXECUTE strSQL	;

  strSQL = ' CREATE TABLE ' + cTableName_DD_rollup + ' (listid int, cDictValue_Rollup  varchar(max)) SORTKEY (listid) ;';
  EXECUTE strSQL	;

  strSQL = ' INSERT INTO ' +cTableName_DD_rollup + ' (listid,cDictValue_Rollup)
  SELECT listid, listagg(DISTINCT cDictValue, '', '') within group (order by listid)  as cDictValue_Rollup
  FROM '+ cTableName_DD + '
  GROUP BY listid
  ORDER BY listid;';
  EXECUTE strSQL	;

  strSQL = 'SELECT  distinct UPPER(cDictValue_Rollup) AS cDictValue_Rollup  from '+  cTableName_DD_rollup +';' ;
  EXECUTE strSQL INTO dictValues;
  dictValues = '{'+ dictValues + '}';
  --IF boolEnableDebug = 1 THEN RAISE INFO 'Dict values: %',  dictValues; END IF;

  --Ready with dict values. now call the function

   IF boolEnableDebug = 1 THEN
       cFieldToUpdate = cFieldToUpdate + '_NEW';
   END IF;


 strSQL = 'UPDATE public.' + cTablename +
  ' SET ' + cFieldToUpdate + ' = udf_getdecodesfromdictionaryarray_multi(UPPER(' + ToBeNormalizedFieldname + '),' + ''',''' +  ','    + ''''  +  dictValues  +'''' + ')
  FROM public.' + cTablename  + ' A
  WHERE Listid = ' + cListid ;

  IF boolEnableDebug = 1 THEN RAISE INFO 'Update sql: %',  strSQL; END IF;
  EXECUTE strSQL	;


  --Cleanup.
  IF boolEnableDebug = 0 THEN
    BEGIN
       strSQL ='DROP TABLE IF EXISTS '+ cTableName_DD + ';';
       EXECUTE strSQL	;
       strSQL = ' DROP TABLE IF EXISTS ' +cTableName_DD_rollup + ';';
       EXECUTE strSQL	;
       RETURN;
    END;
  END IF  ;


END;
$$ LANGUAGE plpgsql;
/




