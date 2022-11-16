/*
 Refresh EXCLUDE_CountriesForSkipEmailValidation
 */


DROP TABLE IF EXISTS Countriestoskip_TobeDropped; COMMIT;


CREATE TABLE  IF NOT EXISTS Countriestoskip_TobeDropped(
		CountryName VARCHAR(60),
		SkipCanadaValidation INT
		)  ;
COMMIT;


COPY Countriestoskip_TobeDropped(
		CountryName ,
		SkipCanadaValidation
	)
from  's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
IGNOREHEADER 0;
COMMIT;

/*
Select * from Countriestoskip_TobeDropped limit 100;
Select count(*) from Countriestoskip_TobeDropped ;
*/

Delete from Countriestoskip_TobeDropped where CountryName is null or ltrim(rtrim(CountryName)) ='';
COMMIT;
CREATE TABLE  IF NOT EXISTS {tablename1}(
		CountryName VARCHAR(60),
		SkipCanadaValidation INT
		)
diststyle all
 compound sortkey (CountryName,SkipCanadaValidation)
;
COMMIT;

--INSERT new entries
INSERT INTO {tablename1}  (CountryName,	SkipCanadaValidation)
SELECT Distinct CountryName,	SkipCanadaValidation
 FROM Countriestoskip_TobeDropped SourceTable
WHERE CountryName NOT IN (Select distinct CountryName from {tablename1}  );
COMMIT;

DROP TABLE IF EXISTS Countriestoskip_TobeDropped;
COMMIT;
/*

Select count(*) from {tablename1};
Select top 100 * from {tablename1};

SELECT * FROM stl_load_errors order by starttime desc LIMIT 100;

*/

