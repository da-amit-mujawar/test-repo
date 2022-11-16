--wireless count report
unload ('select count(*) as recordcount  from  {wireless-tablename}')
to 's3://{s3-internal}{reportname}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;


