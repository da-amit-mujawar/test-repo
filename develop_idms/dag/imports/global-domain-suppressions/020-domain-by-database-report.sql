unload ('Select IDMSID, count(*), count(distinct cDomain) from {tablename1} group by IDMSID order by count(*) desc ')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;COMMIT;