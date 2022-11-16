unload ('select o.* from (SELECT A.* FROM {tablename1} A INNER JOIN {tablename2} B
ON A.LEMS = B.LEMS ORDER BY A.LEMS limit 25) as o')
to 's3://{s3-internal}{reportname3}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off;