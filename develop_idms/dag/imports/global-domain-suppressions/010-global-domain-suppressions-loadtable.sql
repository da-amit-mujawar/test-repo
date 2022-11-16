/*
 Refresh Global domain suppression table.  don't  replace the table
 use update & insert

 Reju Mathew 2021.02.08
 */


DROP TABLE IF EXISTS GlobalDomain_TobeDropped; COMMIT;

CREATE TABLE  GlobalDomain_TobeDropped(
		ID INT Identity,
		cDomain VARCHAR(50),
		IDMSID int,
		datetime_varchar VARCHAR(30),
		dRecordDate DateTime,
		cUser VARCHAR(10)
		)  ;
	COMMIT;


COPY GlobalDomain_TobeDropped(
		cDomain,
		IDMSID ,
		datetime_varchar,
		cUser
	)
from  's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
IGNOREHEADER 0
;COMMIT;


--Delete if any blank lines
Delete from GlobalDomain_TobeDropped where cDomain is null or ltrim(rtrim(cDomain)) =''; commit;
UPDATE GlobalDomain_TobeDropped
     SET cDomain = LTRIM(RTRIM(UPPER(cDomain))),
     dRecordDate = cast(datetime_varchar as timestamp)
; commit;


--create if NOT exists {tablename1} .  don't drop
CREATE TABLE IF NOT EXISTS {tablename1}(
		ID INT Identity,
		cDomain VARCHAR(50),
		IDMSID int,
		dRecordDate DateTime,
		cUser VARCHAR(10)
		)
 DISTSTYLE ALL
 COMPOUND SORTKEY (cDomain,IDMSID);
	COMMIT;


--update to global optout if a domain already  found for a IDMS ID.  0 means applicable to all Dbs
UPDATE {tablename1}
 SET IDMSID = 0,
   dRecordDate = b.dRecordDate,
   cuser= B.cuser
-- select count(*)
--Select A.*, B.*
 FROM {tablename1} A
 INNER JOIN GlobalDomain_TobeDropped B on A.cDomain = B.cDomain and A.IDMSID <>0 and B.IDMSID =0
 ;commit;


--INSERT new entries  --IDMS=0
INSERT INTO {tablename1}  (cDomain,	IDMSID,	dRecordDate, cUser)
SELECT Distinct cDomain,	IDMSID,	dRecordDate, cUser
 FROM GlobalDomain_TobeDropped SourceTable
WHERE IDMSID =0
AND cDomain NOT IN (Select distinct cDomain from {tablename1}  where IDMSID =0 );
Commit;

-- IDMS <>0
INSERT INTO {tablename1} (cDomain,	IDMSID,	dRecordDate, cUser)
 SELECT Distinct cDomain,	IDMSID,	dRecordDate, cUser
 From GlobalDomain_TobeDropped SourceTable Where IDMSID <>0
 AND cDomain NOT IN (Select distinct cDomain from {tablename1}  where IDMSID =0 )
 AND NOT EXISTS ( SELECT  *
              FROM    {tablename1} AS DuplicateTable
          		WHERE   SourceTable.cDomain = duplicateTable.cDomain AND  SourceTable.IDMSID = duplicateTable.IDMSID
          		);

COMMIT;

--drop the staging table
DROP TABLE IF EXISTS GlobalDomain_TobeDropped;
COMMIT;


/*

Select top 100 * from {tablename1} ;
Select count(*) from {tablename1} ;
--313476

Select IDMSID, count(*), count(distinct cDomain) from {tablename1} group by IDMSID order by count(*) desc  ;

--	Select * from stl_load_errors order by starttime desc
select to_date('Dec 27 2012  2:13PM', 'Mon dd YYYY');
Select  cast('Dec 27 2012  2:13PM' as timestamp)

dont' do this. its for testing in DEV only  Drop table {tablename1};


*/