--export QA business email

unload ('select * from {table_business_output_raw_email}')
to 's3://{s3-cdbus-path}{s3-business-export-busemail-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;

--export QA consumer email

unload ('select * from {table_consumer_output_raw_email}')
to 's3://{s3-cdbus-path}{s3-business-export-consemail-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;

--export QA occupations

unload ('select * from {table_business_occupations_output_raw}')
to 's3://{s3-cdbus-path}{s3-business-export-occupat-raw-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;

--export QA sic

unload ('select * from {table_business_output_sic_raw}')
to 's3://{s3-cdbus-path}{s3-business-export-sic-raw-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;

--export QA exec

unload ('select * from {table_business_output_exec_raw}')
to 's3://{s3-cdbus-path}{s3-business-export-exec-raw-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;

--export QA business

unload ('select * from {table_business_output_business_raw}')
to 's3://{s3-cdbus-path}{s3-business-export-business-raw-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;

--export QA final output

unload ('select * from {table_business_output_finalfull_raw}')
to 's3://{s3-cdbus-path}{s3-business-export-business-finalfull-raw-key70}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
parallel off
allowoverwrite
gzip;










