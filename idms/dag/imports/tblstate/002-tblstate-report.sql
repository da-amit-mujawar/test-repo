unload ('select o.* from (SELECT top 10 * from sql_tblState where cstatecode = ''NJ'' union
select top 10 * from sql_tblState where cstatecode = ''AA'' union
select top 10 * from sql_tblState where cstatecode = ''DC'' union
select top 10 * from sql_tblState where cstatecode = ''QC'' union
select top 10 * from sql_tblState where databaseid = 992 union
select top 10 * from sql_tblState where databaseid = 1267 order by databaseid,cstatecode ) as o')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off;
