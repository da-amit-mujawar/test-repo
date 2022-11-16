--sap-email load pipe delimited text file no header
copy {table_sap_email}
from 's3://{s3-cdbus-path}{s3-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Email File Table',count(*),getdate() from {table_sap_email};