/*
Jira 1045


convert sp_AOPScript_B2B as SQL script

--call sp_AOPScript_B2B(1165,'tblMain_21207_202110_rm_tobedropped', 1);

--added to github.  some testing codes are here.
--K:\List Updates - IDMS\AWS Build Migration\Redshift_StoredProcedures

DROP TABLE IF EXISTS tblMain_20610_202011_rm_tobedropped;

SELECT TOP 50000 *
into tblMain_20610_202011_rm_tobedropped
from tblMain_20610_202011;

Select top 100 * from tblMain_20610_202011_rm_tobedropped
Select top 100 * from tblMain_20610_202011;

--call sp_AOPScript_B2B(1165,'tblMain_20610_202011_rm_tobedropped', 1);

Reju Mathew 2021.03.12
*/



CREATE OR REPLACE PROCEDURE sp_AOPScript_B2B (
    Databaseid IN int,
    cTablename  IN VARCHAR(200),
    boolEnableDebug IN int
)
AS $$
DECLARE
  cDatabaseID VARCHAR(20);
  strSQL1 VARCHAR(max) ;
  strSQL2 VARCHAR(max) ;
  strSQL3 VARCHAR(max) ;
  strSQL4 VARCHAR(max) ;
  strSQL5 VARCHAR(max) ;
  strSQL6 VARCHAR(max) ;
  strSQL7 VARCHAR(max) ;
  strSQL8 VARCHAR(max) ;
  strSQL9 VARCHAR(max) ;
  strSQL10 VARCHAR(max) ;

  intRowcount int;
BEGIN
  cDatabaseID = CAST (Databaseid AS VARCHAR(20));

  --apply fix email,  spam, global suppress and Hardbounce and optouts
  strSQL1 =' UPDATE ' + cTablename  + '
    SET emailAddress = udf_FixEmail(emailAddress)
      WHERE emailAddress IS NOT NULL AND emailaddress <> '''' AND emailAddress <> udf_FixEmail(emailAddress);' ;

  strSQL2 ='call sp_suppress_oohb (''' + cTablename  + ''',' +cDatabaseID +',''EMAILADDRESS'', ''SHA256LOWER'', ''CINCLUDE'' , '''');';

  IF boolEnableDebug = 1 THEN
    RAISE INFO 'Variables: 1: %, 2: %,  3: %', cDatabaseID ,cTablename, boolEnableDebug;
    RAISE INFO '%', strSQL1;
    RAISE INFO '%', strSQL2;
  END IF;
  EXECUTE strSQL1	;
  EXECUTE strSQL2	;




  --Fix Country and default some fileds before proceed further
  strSQL1 ='UPDATE ' + cTablename  + '
      SET COUNTRYNAME = udf_fixcountryname(COUNTRYNAME, STATE),
          STATE =  UPPER( STATE),
          COMPANY =  UPPER(COMPANY),
          EncryptedEmail = '''',
          EPD_HasPostal = ''N'',
          EPD_HasPhone  = ''N'',
          GDPRFlag = ''N'',
          TopLevelDomain = '''';'      ;
  --update email related fields
  strSQL2 ='UPDATE ' + cTablename  + ' SET
      EmailAddress_MD5 = udf_hashstring(''MD5'',emailAddress,''LOWER''),
		  SHA256Lower= udf_hashstring(''SHA256'', emailAddress, ''LOWER''),
		  Domain = RIGHT(udf_GetDomain(emailaddress),40),
      Domaintype = udf_GetdomainType(emailAddress)
 		  WHERE emailaddress is not null AND emailaddress <> '''' ;';
  --Update other fields
  strSQL3 ='UPDATE ' + cTablename  + ' SET ZIP = LEFT(ZIPFULL,5)  WHERE CountryName  =''USA''  ;';
  strSQL4 ='UPDATE ' + cTablename  + ' SET ZIP4 = RIGHT(ZIPFULL,4) , Has_Zip4 =''Y''  WHERE CountryName  =''USA'' and LEN (ZIPFULL) >8 ;';
  strSQL5 ='UPDATE ' + cTablename  + ' SET EPD_Location = udf_getLocation(COUNTRYNAME)  ;';
  strSQL6 ='UPDATE ' + cTablename  + ' SET SCF = udf_GetSCF(ZIP,EPD_Location) ;';
  strSQL7 ='UPDATE ' + cTablename  + ' SET EPD_Location  = ''''  WHERE (EPD_Location = ''F'' AND STATE = '''' AND ZIP = '''');';
  strSQL8 ='UPDATE ' + cTablename  + ' SET cInclude = ''Y'' WHERE cInclude is null ;';
  strSQL9 ='UPDATE ' + cTablename  + ' SET cInclude = ''E'' WHERE cInclude in (''Y'',''M'')  AND (EmailAddress is null OR EmailAddress = '''') ;';
  strSQL10 ='UPDATE ' + cTablename  + ' SET EPD_TitleCode = B.cTitleCode, EPD_FunctionCode = B.cFunctionCode  FROM ' + cTablename  + ' a INNER JOIN EXCLUDE_TitleToCode_lookup B on a.Title = B.cTitle;';

  IF boolEnableDebug = 1 THEN
    RAISE INFO '%', strSQL1;
    RAISE INFO '%', strSQL2;
    RAISE INFO '%', strSQL3;
    RAISE INFO '%', strSQL4;
    RAISE INFO '%', strSQL5;
    RAISE INFO '%', strSQL6;
    RAISE INFO '%', strSQL7;
    RAISE INFO '%', strSQL8;
    RAISE INFO '%', strSQL9;
    RAISE INFO '%', strSQL10;
  END IF;
  EXECUTE strSQL1	;
  EXECUTE strSQL2	;
  EXECUTE strSQL3	;
  EXECUTE strSQL4	;
  EXECUTE strSQL5	;
  EXECUTE strSQL6	;
  EXECUTE strSQL7	;
  EXECUTE strSQL8	;
  EXECUTE strSQL9	;
  EXECUTE strSQL10;








  --Update other fields
  strSQL1 ='UPDATE ' + cTablename  + ' SET EPD_TitleCode = '''' WHERE EPD_TitleCode is null;';
  strSQL2 ='UPDATE ' + cTablename + ' SET EPD_FunctionCode ='''' WHERE EPD_FunctionCode is null; ';
  strSQL3 ='UPDATE ' + cTablename  + ' SET EPD_HasPostal = ''Y''
    WHERE (Company <> ''''
    AND LASTNAME <> ''''
    AND FIRSTNAME <> ''''
    AND (AddressLine1 <> ''''  OR AddressLine2 <> '''')
    AND city <> '''' AND STATE <> '''' AND ZIP <> '''')
    AND (LEFT(MailabilityScore, 1) IN (''1'',''2'',''3''));';
  strSQL4 ='UPDATE ' + cTablename + ' SET EPD_HasPhone  = ''Y'' WHERE (Company <> '''' AND LASTNAME <> '''' AND FIRSTNAME <> '''' AND Phone <> '''' ); ';
  strSQL5 ='UPDATE ' + cTablename  + ' SET Individual_ID = LEFT(EmailAddress_md5,17)  WHERE LEFT(MailabilityScore,1) in (''4'',''5'') AND (EmailAddress  is not null AND EmailAddress <>'''' );';
  --Apply SPam and global
  strSQL6 ='call sp_suppress_spamgarbageemails (''' + cTablename  + ''');';
  strSQL7 ='call sp_suppress_globaldomainemail (''' + cTablename  + ''',' +cDatabaseID+');';


  IF boolEnableDebug = 1 THEN
    RAISE INFO '%', strSQL1;
    RAISE INFO '%', strSQL2;
    RAISE INFO '%', strSQL3;
    RAISE INFO '%', strSQL4;
    RAISE INFO '%', strSQL5;
    RAISE INFO '%', strSQL6;
    RAISE INFO '%', strSQL7;
  END IF;
  EXECUTE strSQL1	;
  EXECUTE strSQL2	;
  EXECUTE strSQL3	;
  EXECUTE strSQL4	;
  EXECUTE strSQL5	;
  EXECUTE strSQL6	;
  EXECUTE strSQL7	;

  --Remove Canadian eMail.
  strSQL1 ='UPDATE ' + cTablename  + ' SET
    EncryptedEmail = '''',
		EmailAddress_MD5 = '''',
		cInclude  =''C'',
		emailaddress =''''
    WHERE emailaddress is not null AND emailaddress <> ''''
		      AND cinclude IN (''Y'',''M'')
		      AND (EmailAddress LIKE ''%.CA'' OR  (DOMAIN LIKE ''%.CA'' OR COUNTRYNAME=''CANADA''));';

  strSQL2 ='UPDATE ' + cTablename  + ' SET
    EncryptedEmail = '''',
    EmailAddress_MD5 = '''',
    cInclude  = ''C'',
    emailaddress =''''
    WHERE emailaddress is not null AND emailaddress <> ''''
		    AND cinclude IN (''Y'',''M'')
		    AND (
		       ((COUNTRYNAME=''USA''
	           OR UPPER(COUNTRYNAME) NOT IN(SELECT CountryName from EXCLUDE_CountriesForSkipEmailValidation WHERE SkipCanadaValidation = 1))
	           AND ZipFull <> '''' AND udf_isnumeric(LEFT(ZipFull,5))=0)
	         );';
    strSQL3 ='UPDATE ' + cTablename  + ' SET TopLevelDomain = RIGHT(Domain, LEN(Domain) - CHARINDEX(''.'', Domain) + 1) WHERE Domain <>'''';';
    strSQL4 ='UPDATE ' + cTablename  + ' SET GDPRFlag = ''Y'' WHERE EPD_Location NOT IN (''A'',''B'',''C'');';
    strSQL5 ='UPDATE ' + cTablename  + ' SET GDPRFlag = ''Y''
      WHERE EPD_Location in (''A'',''B'',''C'')
		  AND TopLevelDomain  in (''.AD'',''.AL'',''.AM'',''.AT'',''.AZ'',''.BA'',''.BE'',''.BG'',''.BY'',''.CAT'',''.CH'',''.CY'',''.CZ'',
		                     ''.DE'',''.DK'',''.EE'',''.ES'',''.EU'',''.FI'',''.FR'',''.GE'',''.GG'',''.GL'',''.GR'',''.HR'',''.HU'',
		                    ''.IE'',''.IM'',''.IS'',''.IT'',''.JE'',''.KZ'',''.LI'',''.LT'',''.LU'',''.LV'',''.MC'',''.MD'',
		                    ''.ME'',''.MK'',''.MT'',''.NL'',''.NO'',''.PL'',''.PT'',''.RO'',''.RS'',''.RU'',''.SE'',''.SI'',
		                    ''.SK'',''.SM'',''.SU'',''.TR'',''.UA'',''.UK'',''.VA'');  ';


  IF boolEnableDebug = 1 THEN
    RAISE INFO '%', strSQL1;
    RAISE INFO '%', strSQL2;
    RAISE INFO '%', strSQL3;
    RAISE INFO '%', strSQL4;
    RAISE INFO '%', strSQL5;
  END IF;
  EXECUTE strSQL1	;
  EXECUTE strSQL2	;
  EXECUTE strSQL3	;
  EXECUTE strSQL4	;
  EXECUTE strSQL5	;


END;
$$ LANGUAGE plpgsql;
/




