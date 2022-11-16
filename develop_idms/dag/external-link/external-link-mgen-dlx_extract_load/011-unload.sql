unload ('select  * from {tablename1} where LEMS in (select LEMS from {tablename1} limit 100)')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
header
parallel off
;
