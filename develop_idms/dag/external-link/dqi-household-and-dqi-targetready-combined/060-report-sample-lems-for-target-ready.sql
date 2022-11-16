unload ('select o.* from (SELECT A.* FROM {tablename2} A INNER JOIN {tablename1} B
ON A.LEMS = B.LEMS ORDER BY A.LEMS limit 25) as o')
to 's3://{s3-internal}{reportname4}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off;

