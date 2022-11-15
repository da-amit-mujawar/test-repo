--export b2c db intent link file

unload ('select * from {table_intent_b2c}') 
to 's3://{s3-internal}{s3-key8}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|' 
allowoverwrite
gzip;


insert into {table_job_stats} values
('Exported B2C Link File',0,getdate() );
