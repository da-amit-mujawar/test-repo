

/*
Reju Mathew 2021.02.25.
Jira 663

Load the OOHB file to Temp Table, cleanup.
Create EXCLUDE_OOHB_xxxx  Table if not found
Insert to table if Email or sha256 not inserted before.
show number of records inserted.

select * from EXCLUDE_OOHB_{DATABASEID} limit 100;

*/


/*
dont use this for production.
--this is for testing.   DROP TABLE IF EXISTS EXCLUDE_OOHB_{DATABASEID};
*/


--Create the OOHB table if not found
CREATE TABLE IF NOT EXISTS EXCLUDE_OOHB_{DATABASEID} (
    ID bigint identity,
    EmailAddress VARCHAR(65),
    Listid int,
    cUser VARCHAR(10),
    dRecorddate DateTime,
    cFlag VARCHAR(1),
    SHA256Lower VARCHAR(64)
)
 COMPOUND SORTKEY (SHA256Lower,Listid);




--Load Exported file into temp table
DROP TABLE IF EXISTS #OOHB;

CREATE TEMPORARY TABLE #OOHB (
    EmailAddress VARCHAR(65) ,
    Listid_var VARCHAR(8),
    cUser VARCHAR(10),
    DRecorddate_var VARCHAR (20),
    cFlag VARCHAR(1),
    SHA256Lower VARCHAR(64),
    Listid int,
    DRecorddate Datetime
 )
 COMPOUND SORTKEY (SHA256Lower,EmailAddress);

COPY #OOHB
from  's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
fixedwidth 'EmailAddress:65, Listid_var:8, cUser:10, DRecorddate_var:20,  cFlag:1,  SHA256Lower: 64'
IGNOREHEADER 0
ACCEPTINVCHARS
IGNOREBLANKLINES
NULL AS '\\0'
timeformat 'auto'
;

/*
select count(*) from #oohb;
select * from #oohb limit 100;
*/

/* remove invalid chars*/
DELETE from #oohb where udf_isnumeric (listid_var) =0 ;


UPDATE #OOHB  SET
    EmailAddress = UPPER(TRIM(EmailAddress)),
    Listid = CASE WHEN Trim(Listid_var) <> '' THEN CAST (Listid_var  as int) else 0 end,
    DRecorddate = CASE WHEN trim(DRecorddate_var) <>'' THEN CAST (DRecorddate_var  as datetime) else getdate() end
 FROM #OOHB;


/*
Select top 100 * from #OOHB;
_TONYPHILLIPS@EXCITE.COM
2d66fb0e35e70dc0ae4835a88c3097e6d147385b0e2dcd9b0a51912c63326254

*/



--OO  insert 1-- NOT pubspecific
INSERT INTO EXCLUDE_OOHB_{DATABASEID} (EmailAddress,Listid,cUser,DRecorddate,cFlag, SHA256Lower )
 SELECT Distinct EmailAddress,Listid,cUser,DRecorddate,cFlag, SHA256Lower
 From #OOHB  Where cFlag ='O'   AND Listid =0
 AND SHA256Lower NOT IN (Select distinct SHA256Lower from EXCLUDE_OOHB_{DATABASEID}  where cFlag ='O' AND Listid =0 )
;



--OO  insert 1-- pub specific
INSERT INTO EXCLUDE_OOHB_{DATABASEID} (EmailAddress,Listid,cUser,DRecorddate,cFlag, SHA256Lower )
 SELECT Distinct EmailAddress,Listid,cUser,DRecorddate,cFlag, SHA256Lower
 From #OOHB SourceTable Where cFlag ='O'   AND Listid <>0
 AND SHA256Lower NOT IN (Select distinct SHA256Lower from EXCLUDE_OOHB_{DATABASEID}  where cFlag ='O' AND Listid =0 )
 AND NOT EXISTS ( SELECT  *
        FROM    EXCLUDE_OOHB_{DATABASEID} AS DuplicateTable
		WHERE   SourceTable.SHA256Lower = duplicateTable.SHA256Lower
		AND  SourceTable.listid = duplicateTable.listid );



--HB insert, only if not previously inserted.    H cant be pub specific
INSERT INTO EXCLUDE_OOHB_{DATABASEID} (EmailAddress,Listid,cUser,DRecorddate,cFlag, SHA256Lower )
 SELECT Distinct EmailAddress,0 as Listid,cUser,DRecorddate,cFlag, SHA256Lower
 From #OOHB  Where cFlag ='H'
 AND SHA256Lower NOT IN (Select Distinct SHA256Lower from EXCLUDE_OOHB_{DATABASEID}  where cFlag ='H' )
;




/*
Select top 100 * from EXCLUDE_OOHB_{DATABASEID};

Select cFlag, count(*)
from EXCLUDE_OOHB_{DATABASEID}
group by cFlag;

*/


/*

Select count(*) from EXCLUDE_OOHB_{DATABASEID};
Select top 100 * from EXCLUDE_OOHB_{DATABASEID};

SELECT * FROM stl_load_errors order by starttime desc LIMIT 100;

*/


