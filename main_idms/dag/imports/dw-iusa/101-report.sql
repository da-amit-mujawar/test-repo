UNLOAD ('select INFO_STATE as State, count(*) as nCount from {maintable_name} group by INFO_STATE order by 1')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;
