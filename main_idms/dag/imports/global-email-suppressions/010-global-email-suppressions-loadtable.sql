
DROP TABLE IF EXISTS GlobalEmail_TobeDropped; COMMIT;

CREATE TABLE  IF NOT EXISTS GlobalEmail_TobeDropped(
		ID INT Identity,
		cEmail VARCHAR(85),
		cRemType VARCHAR(1),
		datetime_varchar VARCHAR(30),
		dRecordDate DateTime,
		cUser VARCHAR(10)
		);COMMIT;



COPY GlobalEmail_TobeDropped(
		cEmail,
		cRemType ,
		datetime_varchar ,
		cUser
	)
from  's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
IGNOREHEADER 0;
COMMIT;

/*
Select * from GlobalEmail_TobeDropped limit 100;
Select count(*) from GlobalEmail_TobeDropped ;
*/


--Delete if any blank lines
Delete from GlobalEmail_TobeDropped where cEmail is null or ltrim(rtrim(cEmail)) ='';
UPDATE GlobalEmail_TobeDropped
  SET cEmail = LTRIM(RTRIM(UPPER(cEmail))),
  dRecordDate = cast(datetime_varchar as timestamp)  ;
COMMIT;



CREATE TABLE  IF NOT EXISTS {tablename1}(
		ID INT Identity,
		cEmail VARCHAR(85),
		cRemType VARCHAR(1),
		dRecordDate DateTime,
		cUser VARCHAR(10)
		)
		DISTSTYLE ALL
 COMPOUND SORTKEY (cEmail,cRemType);
COMMIT;

--INSERT new entries
INSERT INTO {tablename1}  (cEmail,	cRemType,	dRecordDate, cUser)
SELECT Distinct cEmail,	cRemType,	dRecordDate, cUser
 FROM GlobalEmail_TobeDropped SourceTable
WHERE cEmail NOT IN (Select distinct cEmail from {tablename1}  );
COMMIT;

DROP TABLE IF EXISTS GlobalEmail_TobeDropped;
COMMIT;

/*

Select count(*) from {tablename1};
Select top 100 * from {tablename1};

SELECT * FROM stl_load_errors order by starttime desc LIMIT 100;

*/


