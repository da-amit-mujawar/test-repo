unload ('Select cremtype, count(*), count(distinct cemail) from {tablename1} group by cremtype order by count(*) desc  ; ')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;COMMIT;