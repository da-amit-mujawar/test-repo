--create e-hygiene reports

unload ('select emailscoreflag,count(*) from {maintable_name} group by emailscoreflag order by emailscoreflag')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;

unload ('select eoverifiedemail,count(*) from {maintable_name} group by eoverifiedemail order by eoverifiedemail')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;

unload ('select email_Suppression_Flag,count(*) from {maintable_name} group by email_Suppression_Flag order by email_Suppression_Flag')
to 's3://{s3-internal}{reportname3}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;

unload ('select emailable,count(*) from {maintable_name} group by emailable order by emailable')
to 's3://{s3-internal}{reportname4}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;
