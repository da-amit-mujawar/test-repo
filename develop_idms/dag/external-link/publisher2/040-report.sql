unload ('select o.* from (SELECT * FROM {tablename1} where PUB2SOURCE =''Y'' limit 100) as o')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;