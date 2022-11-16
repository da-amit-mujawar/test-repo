unload ('select count(*) as recordcount  from  {maintable_name}')
to 's3://{s3-internal}{reportname}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;