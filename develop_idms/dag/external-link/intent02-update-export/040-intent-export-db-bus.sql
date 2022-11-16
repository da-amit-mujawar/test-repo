--export business db intent link file

unload ('select * from {table_intent_us_bus}')
to 's3://{s3-internal}{s3-key7}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|' 
allowoverwrite
gzip;

insert into {table_job_stats} values
('Exported US-Bus File',0,getdate() );
