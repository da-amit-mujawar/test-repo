unload('Select State, count(*) as ncount from tblCobraWeekly881_Final group by State order by State')
TO 's3://{s3-internal}{reportname1}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;


unload('SELECT * FROM tblCobraWeekly881_Final WHERE id IN (SELECT id FROM tblCobraweekly881_final LIMIT 100)')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;

