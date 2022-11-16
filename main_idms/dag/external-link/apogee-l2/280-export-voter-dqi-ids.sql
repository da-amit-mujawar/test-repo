unload ('select o.* from (SELECT Individual_ID, L2_LALVOTERID
FROM {tablename1} where individual_id < 5000000000000) as o')
to 's3://{s3-internal}{reportname4}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;