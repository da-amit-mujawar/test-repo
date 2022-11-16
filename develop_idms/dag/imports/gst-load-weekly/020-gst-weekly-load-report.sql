unload ('select count(*) from {tablename1}')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;


unload ('select suppression_type,count(*) from {tablename1} group by suppression_type')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;