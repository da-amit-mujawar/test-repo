--export sapphire db intent link file

unload ('select * from {table_intent_sapp}') 
to 's3://{s3-internal}{s3-key9}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|' 
allowoverwrite
gzip;


insert into {table_job_stats} values
('Exported Sapp File',0,getdate() );