UNLOAD ('SELECT COUNT(*) AS recordcount FROM {maintable_name}')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;

UNLOAD ('SELECT * FROM {maintable_name} WHERE id IN (SELECT id FROM {maintable_name} LIMIT 100)')
TO 's3://{s3-internal}{reportname2}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;

DROP TABLE IF EXISTS {table-businessemail-new-raw};

-- Export the whole table to data-lake. This extract is used by 992 DAG
UNLOAD ('SELECT * FROM {maintable_name}')
TO 's3://{s3-axle-gold}/business-email/full'
IAM_ROLE '{iam}'
FORMAT CSV
DELIMITER '|'
GZIP 
CLEANPATH
PARALLEL ON
MAXFILESIZE 200 MB
;


