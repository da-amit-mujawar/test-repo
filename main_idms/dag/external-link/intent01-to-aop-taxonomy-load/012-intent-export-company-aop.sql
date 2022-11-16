--export company file to aop

unload ('select * from {table_company_raw}')
to 's3://{s3-aopinput}{s3-key0}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|' 
allowoverwrite
gzip;
