--rename table for loading to redshift
DROP TABLE IF EXISTS {wireless-tablename}_previous;
ALTER TABLE {wireless-tablename} RENAME to {wireless-tablename}_previous;
ALTER TABLE {wireless-tablename1} RENAME to  {wireless-tablename};

UNLOAD ('SELECT * FROM {wireless-tablename}')
TO 's3://{s3-axle-gold}/wireless-parquet/'
IAM_ROLE '{iam}'
CLEANPATH
FORMAT PARQUET;





