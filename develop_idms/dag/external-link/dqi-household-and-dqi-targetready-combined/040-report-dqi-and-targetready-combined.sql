unload ('select o.* from (SELECT * FROM {tablename2} limit 100) as o')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off;