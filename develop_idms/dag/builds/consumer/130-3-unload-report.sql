DROP TABLE IF EXISTS nosuchtable;
UNLOAD ('SELECT * FROM {maintable_name} WHERE id IN (SELECT id FROM {maintable_name} LIMIT 1000)')
TO 's3://{s3-internal}{reportname1}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;