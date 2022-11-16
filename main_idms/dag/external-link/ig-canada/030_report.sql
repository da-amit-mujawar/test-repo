UNLOAD ('SELECT * FROM {maintable_name} WHERE id IN (SELECT id FROM {maintable_name} LIMIT 100)')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;
